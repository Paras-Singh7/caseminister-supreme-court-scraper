import shutil
import logging.config
import os
import re
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import PyPDF2
import requests
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from msrest.authentication import CognitiveServicesCredentials
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

load_dotenv()

# Setting up mongo db client
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB_NAME")]
collection = db[os.getenv("MONGO_COLLECTION_NAME")]

# Azure cognitive service setup
subscription_key = os.getenv("COMPUTER_VISION_CLIENT_SUBSCRIPTION_KEY")
endpoint = os.getenv("COMPUTER_VISION_CLIENT_ENDPOINT")

client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))

### Logger Setup
LOG_DIR = "logs"
logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "verbose": {
                "format": "{levelname} {asctime} {module} {message}",
                "style": "{",
            },
            "simple": {
                "format": "{levelname} {message}",
                "style": "{",
            },
        },
        "handlers": {
            "file_supremecourt": {
                "level": "DEBUG",
                "class": "logging.FileHandler",
                "filename": os.path.join(LOG_DIR, "supremecourt.log"),
                "formatter": "verbose",
            },
        },
        "supremecourt": {
            "authentication": {
                "handlers": ["file_supremecourt"],
                "level": "DEBUG",
                "propagate": False,
            },
        },
    }
)
logger = logging.getLogger("supremecourt")


def process_case_details_by_diary_number(diary_number: str, year: str):
    driver = get_headless_driver()
    wait = WebDriverWait(driver, 10)
    try:
        driver.get("https://www.sci.gov.in/case-status-diary-no/")

        diary_number_field = wait.until(EC.presence_of_element_located((By.ID, "diary_no")))
        diary_number_field.send_keys(diary_number)

        select_year_element = wait.until(EC.presence_of_element_located((By.ID, "year")))
        select_year = Select(select_year_element)
        select_year.select_by_value(year)

        data = retry_captcha_process(driver, wait)

        return data
    finally:
        driver.quit()
        pass


def process_case_details_by_case_number(case_type: str, case_no: str, year: str):
    driver = get_headless_driver()
    wait = WebDriverWait(driver, 10)
    try:

        driver.get("https://www.sci.gov.in/case-status-case-no/")

        select_element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "case_type")))
        select = Select(select_element)
        select.select_by_visible_text(case_type)

        input_field = wait.until(EC.presence_of_element_located((By.ID, "case_no")))
        input_field.send_keys(case_no)

        select_year_element = wait.until(EC.presence_of_element_located((By.ID, "year")))
        select_year = Select(select_year_element)
        select_year.select_by_value(year)

        data = retry_captcha_process(driver, wait)

        return data
    finally:
        driver.quit()
        pass


def get_headless_driver():
    try:
        options = Options()
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920x1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        )
        return webdriver.Chrome(options=options)
    except Exception as e:
        logger.error(f"Error in getting headless_driver: {e}")
        raise Exception(f"Error in getting headless_driver: {e}")


def retry_captcha_process(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    attempt: int = 0,
    max_attempts: int = 20,
):
    try:
        if attempt >= max_attempts:
            logger.error("Max CAPTCHA attempts reached. Exiting process.")
            raise Exception("Max CAPTCHA attempts reached. Please try again.")

        time.sleep(2)
        captcha_image_element = wait.until(EC.presence_of_element_located((By.ID, "siwp_captcha_image_0")))

        captcha_image_path = f"{uuid.uuid4().hex}.png"
        time.sleep(2)
        captcha_image_element.screenshot(captcha_image_path)

        extracted_text = extract_text_from_image(captcha_image_path)
        result = solve_expression(extracted_text)
        logger.info(result)

        # Delete the CAPTCHA image after extracting text
        if os.path.exists(captcha_image_path):
            os.remove(captcha_image_path)
            logger.info(f"Captcha image {captcha_image_path} deleted.")

        if isinstance(result, str) and result.startswith("Error evaluating expression"):
            logger.error("Failed to parse expression. Retrying CAPTCHA process...")
            refresh_link = driver.find_element(By.CLASS_NAME, "captcha-refresh-btn")
            refresh_link.click()
            retry_captcha_process(driver, wait, attempt + 1)
        else:
            captcha_input_field = wait.until(EC.presence_of_element_located((By.ID, "siwp_captcha_value_0")))
            captcha_input_field.clear()

            captcha_input_field.send_keys(str(result))
            submit_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='submit'  and @value='Search']"))
            )
            submit_button.click()

            try:
                try:
                    WebDriverWait(driver, 3).until(
                        EC.visibility_of_element_located(By.XPATH, "//div[@class='notfound']")
                    )
                    return False
                except Exception as e:
                    pass

                dist_table_content = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "distTableContent"))
                )

                table_element = dist_table_content.find_element(By.XPATH, ".//table")
                return process_table(driver, table_element)
            except Exception as e:
                logger.error(f"Error after form submission: {str(e)}")

    except Exception as e:
        logger.error(f"Error in retry_captcha_process: {str(e)}")
        traceback.print_exc()


def extract_text_from_image(image_path: str):
    try:
        with open(image_path, "rb") as image_data:
            read_response = client.read_in_stream(image_data, raw=True)

        operation_location = read_response.headers["Operation-Location"]
        operation_id = operation_location.split("/")[-1]

        while True:
            get_text_results = client.get_read_result(operation_id)
            if get_text_results.status not in [OperationStatusCodes.running, OperationStatusCodes.not_started]:
                break
            time.sleep(1)

        if get_text_results.status == OperationStatusCodes.succeeded:
            text = ""
            for page_result in get_text_results.analyze_result.read_results:
                for line in page_result.lines:
                    text += line.text + "\n"
            return text.strip()
        else:
            return "Failed to read text from image."
    except Exception as e:
        logger.error(f"Error extracting text from image: {e}")
        traceback.print_exc()
        return "Failed to read text from image."


def solve_expression(text: str):
    try:
        parts = text.split("+") if "+" in text else text.split("-")
        operator = "+" if "+" in text else "-"
        operands = [part.strip() for part in parts]

        if len(operands) != 2:
            raise ValueError("Invalid expression format")

        operand1, operand2 = map(int, operands)

        result = operand1 + operand2 if operator == "+" else operand1 - operand2
        return result

    except Exception as e:
        logger.error(f"Error evaluating expression: {e}")
        return f"Error evaluating expression: {e}"


def process_table(driver, table_element):
    rows = table_element.find_elements(By.XPATH, ".//tbody/tr")
    for row in rows:
        data = {}

        diary_number = row.find_element(By.XPATH, ".//td[@data-th='Diary Number']/span").text
        case_number = row.find_element(By.XPATH, ".//td[@data-th='Case Number']/span").text
        petitioner_name = row.find_element(By.XPATH, ".//td[@class='petitioners']/span").text.strip()
        respondent_name = row.find_element(By.XPATH, ".//td[@class='respondents']/span").text.strip()
        status = row.find_element(By.XPATH, ".//td[@data-th='Status']/span").text

        view_link = row.find_element(By.XPATH, ".//td[@data-th='Action']/span/a")
        time.sleep(2)
        view_link.click()

        element = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, "cnrResultsDetails")))
        driver.execute_script("arguments[0].click();", element)

        details_div = driver.find_element(By.ID, "cnrResultsDetails")
        html_content = details_div.get_attribute("outerHTML")
        soup = BeautifulSoup(html_content, "html.parser")

        case_details = extract_case_details(soup)
        data["details"] = {
            "diary_number": diary_number,
            "petitioner_name": petitioner_name,
            "respondent_name": respondent_name,
            "case_status": status,
            "case_number": case_details.get("Case Number", ""),
            "category": case_details.get("Category", ""),
            "filing_date": case_details.get("Filing Date", ""),
            "registration_date": case_details.get("Registration Date", ""),
            "listing_date": (
                case_details.get("Present/Last Listed On", "")
                or case_details.get("Tentatively case may be listed on", "")
                or case_details.get("likely to be listed on", "")
            ),
            "stage": case_details.get("Stage", ""),
            "petitioner_advocate": case_details.get("Petitioner Advocate(s)", ""),
            "respondent_advocate": case_details.get("Respondent Advocate(s)", ""),
        }

        logger.info("===========================================================================")
        data["earlier_court_details"] = click_and_extract_earlier_court_details(driver)
        logger.info(f"Earlier Court Details JSON:{data['earlier_court_details']}")

        data["listing_date"] = extract_listing_dates(driver)
        logger.info(f"Extract Listing Dates JSON:{data['listing_date']}")

        data["interlocutory_application_documents"] = interlocutory_application_documents(driver)
        logger.info(f"Interlocutory Application Documents JSON:{data['interlocutory_application_documents']}")

        data["notices"] = notices(driver)
        logger.info(f"Notices JSON:{data['notices']}")

        data["defects"] = defects(driver)
        logger.info(f"Defects JSON:{data['defects']}")

        data["mention_memo"] = mention_memo(driver)
        logger.info(f"Mention Memo JSON:{data['mention_memo']}")

        data["office_report"] = office_report(driver)
        logger.info(f"Office Report JSON:{data['office_report']}")

        data["tagged_matters"] = tagged_matters(driver)
        logger.info(f"Tagged Matters JSON:{data['tagged_matters']}")

        judgement_orders_data, merged_pdf_path, folder_pdf_paths = judgement_orders(diary_number, driver)
        for order_item in judgement_orders_data:
            saved_path = order_item["url"]
            new_url = upload_pdf_to_azure(saved_path, data)
            order_item["url"] = new_url

        data["judgement_orders"] = judgement_orders_data

        new_merged_pdf_path = upload_pdf_to_azure(merged_pdf_path, data)
        data["merge_pdf_url"] = new_merged_pdf_path

        remove_dir(folder_pdf_paths)

        logger.info(f"Judgement Orders JSON:{judgement_orders_data}")

        return data


def extract_case_details(soup):
    details = {}
    table = soup.find("table", class_="caseDetailsTable")

    if not table:
        return None

    for row in table.find_all("tr"):
        cells = row.find_all("td", limit=2)
        if len(cells) == 2:
            key = cells[0].text.strip().rstrip(":")
            value = cells[1].text.strip()
            details[key] = value

    return details or None


def extract_table_data(table: BeautifulSoup):
    table = table.find("tbody").find("table")

    header = [th.get_text(strip=True) for th in table.find("thead").find_all("th") if th.get_text(strip=True)]
    if not header:
        header = [th.get_text(strip=True) for th in table.find("thead").find_all("td") if th.get_text(strip=True)]

    rows = []
    tbody = table.find("tbody")
    if tbody:
        for row in tbody.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td") if td.get_text(strip=True)]
            if cells:  # Only append non-empty rows
                rows.append(cells)

    # Prepare and return JSON data
    data = {"header": header, "rows": rows}
    return data


def extract_nested_table_data(table: BeautifulSoup):
    table = table.find("tbody").find("table")
    table_names = table.find_all("strong")
    nested_tables = table.find_all("table")

    data = []
    for name, table in zip(table_names, nested_tables):
        cleaned_name = clean_header(name.text.strip())
        header = [th.get_text(strip=True) for th in table.find("thead").find_all("th") if th.get_text(strip=True)]
        rows = []
        tbody = table.find("tbody")
        if tbody:
            for row in tbody.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all("td") if td.get_text(strip=True)]
                if cells:  # Only append non-empty rows
                    rows.append(cells)
        data.append({cleaned_name: {"header": header, "rows": rows}})

    return data


def extract_table_details(driver: webdriver.Chrome, table_class: str, timeout: int = 10, nested=False):
    try:
        button_xpath = f"//table[contains(@class, '{table_class}')]//button"
        table_xpath = f"//table[contains(@class, '{table_class}')]//button"

        time.sleep(2)

        # Click the button and wait for the table to be visible
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, button_xpath))).click()
        time.sleep(1)
        WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.XPATH, table_xpath)))
        time.sleep(2)

        # Extract and parse the table
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", class_=table_class)

        if not table:
            logger.warning(f"No table found with class '{table_class}' at {driver.current_url}")
            return None

        if not nested:
            return extract_table_data(table)
        else:
            return extract_nested_table_data(table)

    except Exception as e:
        logger.error(f"Error extracting table details for class '{table_class}': {e}")
        return None


def clean_header(header):
    # Process slashes and spaces
    header = re.sub(r"\s*/\s*", "_|_", header)
    # Replace 'No.' variations with 'number'
    header = re.sub(r"\bno\.?\b", "number", header, flags=re.IGNORECASE)
    # Convert to lowercase and clean special characters
    header = header.lower()
    header = re.sub(r"[^a-z0-9|_ ]", "", header)
    # Format underscores and spaces
    header = header.replace(" ", "_")
    header = re.sub(r"_+", "_", header)
    header = header.strip("_")
    return header


def clean_headers(headers):
    cleaned = []
    for header in headers:
        cleaned.append(clean_header(header))
    return cleaned


def process_table_data(data: dict, table_class: str):
    if not data or "header" not in data or "rows" not in data:
        logger.warning(f"Invalid/missing data for table: {table_class}")
        return []

    # Clean headers and handle empty/mismatch cases
    cleaned_headers = clean_headers(data["header"]) if data["header"] else []
    if not cleaned_headers:
        logger.warning(f"No headers found for table: {table_class}")
        return []

    processed_rows = []
    for row_idx, row in enumerate(data["rows"]):
        if not row:  # Skip empty rows
            continue

        # Ensure header-cell count match
        cells = row[: len(cleaned_headers)]  # Truncate extra cells
        if len(cells) < len(cleaned_headers):
            cells += [""] * (len(cleaned_headers) - len(cells))  # Pad missing cells

        processed_rows.append({hdr: cell.strip() if cell else None for hdr, cell in zip(cleaned_headers, cells)})

    return processed_rows


def click_and_extract_earlier_court_details(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable earlier_court_details no-responsive", timeout)
    return process_table_data(data=data, table_class="earlier_court_details")


def extract_listing_dates(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable listing_dates no-responsive", timeout)
    return process_table_data(data=data, table_class="listing_dates")


def interlocutory_application_documents(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(
        driver, "caseDetailsTable interlocutory_application_documents no-responsive", timeout, True
    )
    if data:
        for idx, dict in enumerate(data):
            for k, v in dict.items():
                data[idx][k] = process_table_data(data=v, table_class="interlocutory_application_documents")
        return data
    return None


def notices(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable notices no-responsive", timeout)
    return process_table_data(data=data, table_class="notices")


def defects(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable defects no-responsive", timeout)
    return process_table_data(data=data, table_class="defects")


def mention_memo(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable mention_memo no-responsive", timeout)
    return process_table_data(data=data, table_class="mention_memo")


def office_report(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable office_report no-responsive", timeout)
    return process_table_data(data=data, table_class="office_report")


def tagged_matters(driver: webdriver.Chrome, timeout: int = 10):
    data = extract_table_details(driver, "caseDetailsTable tagged_matters no-responsive", timeout)
    return process_table_data(data=data, table_class="tagged_matters")


def download_pdf(link, folder_path):
    if not link or not link.get("href"):
        return None

    url = link["href"]
    pdf_name = uuid.uuid4().hex + url.split("/")[-1]
    pdf_path = os.path.join(folder_path, pdf_name)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(pdf_path, "wb") as f:
            f.write(response.content)
        return pdf_path
    except Exception as e:
        logger.error(f"Could not download pdf '{pdf_name}': {e}")
        return None


def extract_date_from_filename(filename):
    try:
        date_part = os.path.basename(filename).split("Order_")[-1].replace(".pdf", "")
        return datetime.strptime(date_part, "%d-%b-%Y")
    except (ValueError, IndexError):
        return datetime.min


def merge_pdfs(pdf_files, output_path):
    pdf_writer = PyPDF2.PdfWriter()
    for pdf in pdf_files:
        try:
            pdf_reader = PyPDF2.PdfReader(pdf)
            for page in pdf_reader.pages:
                pdf_writer.add_page(page)
        except Exception as e:
            logger.error(f"Error reading PDF {pdf}: {e}")
    with open(output_path, "wb") as merged_pdf:
        pdf_writer.write(merged_pdf)


def judgement_orders(diary_number: str, driver: webdriver.Chrome, timeout: int = 10):
    try:
        selector = "caseDetailsTable judgement_orders no-responsive"

        button_xpath = f"//table[contains(@class, '{selector}')]//button"
        table_xpath = f"//table[contains(@class, '{selector}')]//button"

        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, button_xpath))).click()
        time.sleep(2)
        WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.XPATH, table_xpath)))

        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", class_="caseDetailsTable judgement_orders no-responsive")

        if not table:
            logger.warning(f"No table found with class '{selector}' at {driver.current_url}")
            return None, None, None

        # Downloading order pdfs
        folder_name = diary_number.replace("/", "_")
        folder_path = os.path.join("case_pdfs", folder_name)
        os.makedirs(folder_path, exist_ok=True)

        links = [row.find("a") for row in table.find("tbody").find_all("tr")]

        with ThreadPoolExecutor(max_workers=5) as executor:
            pdf_files = list(filter(None, executor.map(lambda link: download_pdf(link, folder_path), links)))

        if not pdf_files:
            logger.warning(f"No pdf files found at {driver.current_url}")
            return [], None, folder_path

        pdf_files.sort(key=lambda x: extract_date_from_filename(x))
        merged_pdf_path = os.path.join(folder_path, f"{folder_name}_merged_pdf.pdf")
        merge_pdfs(pdf_files, merged_pdf_path)

        table_data = [
            {"order_date": datetime.strftime(extract_date_from_filename(pdf), "%d/%m/%Y"), "url": pdf}
            for pdf in pdf_files
        ]

        return table_data, merged_pdf_path, folder_path

    except Exception as e:
        logger.error(f"Error extracting earlier court details: {e}")
        return None, None


def upload_pdf_to_azure(file_path, details):
    try:
        blob_name = file_path.split("/")[-1]
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_CONNECTION_STRING"))
        blob_client = blob_service_client.get_blob_client(container=os.getenv("AZURE_CONTAINER_NAME"), blob=blob_name)
        if blob_client.exists():
            blob_client.delete_blob()
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        return blob_client.url
    except Exception as e:
        logger.error(f"Error while uploading {file_path} of case {details} to azure: {e}")
        return None


def remove_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


def save_to_mongodb(data):
    collection.insert_one(data)


if __name__ == "__main__":
    from_year = 2025

    while from_year >= 2020:
        current_diary_number = 1
        continuous_empty_case = 0

        while True:
            op = process_case_details_by_diary_number(str(current_diary_number), str(from_year))

            if continuous_empty_case == 10:
                break

            if op:
                op["year"] = from_year
                save_to_mongodb(op)
            else:
                continuous_empty_case += 1

            current_diary_number += 1

        from_year -= 1
