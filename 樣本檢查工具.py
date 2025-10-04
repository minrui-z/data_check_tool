import customtkinter as ctk
import tkinter as tk
from tkinter import messagebox, filedialog
import os
import sys
import re
import csv
import logging
from typing import List, Dict, Tuple, Optional, Set
from urllib.parse import urljoin
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import pandas as pd
import threading
import time

# =================================================================
# 核心爬蟲與檢查邏輯 (與 V2.2 保持一致，僅替換了 GUI 庫)
# =================================================================

# ---------------------- Basic Config ----------------------
BASE_URL = "https://esccapi.nccu.edu.tw"
LIST_PATH_TMPL = "/admin/project/{project}/wave/{wave}/survey-work/list?page={page}"
EDIT_BASE_TMPL = "/admin/project/{project}/wave/{wave}/survey-work/edit/{work_id}"

MAX_WORKERS = 15
TIMEOUT = 15

# 爬蟲的日誌器
crawler_logger = logging.getLogger("Crawler")
crawler_logger.setLevel(logging.INFO)
if not crawler_logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch.setFormatter(formatter)
    crawler_logger.addHandler(ch)

# ---------------------- Session Factory ----------------------
def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": BASE_URL
    })
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS * 2,
        max_retries=3
    )
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    return s

# ---------------------- Login ----------------------
def fetch_csrf_and_login(session: requests.Session, email: str, password: str) -> None:
    # 邏輯與 v6.0.1 相同
    login_path_candidates = ["/admin/login", "/admin/auth/login", "/login", "/auth/login"]
    last_err: Optional[Exception] = None

    for p in login_path_candidates:
        try:
            login_url = urljoin(BASE_URL, p)
            r = session.get(login_url, timeout=TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            last_err = e
            continue

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form")
        if not form:
            post_url = login_url
            payload = {"email": email, "password": password}
            r2 = session.post(post_url, data=payload, timeout=TIMEOUT, allow_redirects=True)
            if r2.status_code in (200, 302):
                probe = session.get(urljoin(BASE_URL, "/admin"), timeout=TIMEOUT, allow_redirects=True)
                if probe.status_code == 200 and "admin" in probe.url:
                    crawler_logger.info("登入成功")
                    return
            last_err = RuntimeError(f"Login failed at {post_url}")
            continue

        action = form.get("action") or p
        action_url = urljoin(BASE_URL, action)
        payload: Dict[str, str] = {}
        for inp in form.select("input"):
            name = inp.get("name")
            if not name:
                continue
            payload[name] = inp.get("value", "")

        if soup.select_one('input[name="user[email]"]') or "user[email]" in payload:
            payload["user[email]"] = email
            payload["user[password]"] = password
        else:
            payload["email"] = email
            payload["password"] = password

        r2 = session.post(action_url, data=payload, timeout=TIMEOUT, allow_redirects=True)
        if r2.status_code in (200, 302):
            probe = session.get(urljoin(BASE_URL, "/admin"), timeout=TIMEOUT, allow_redirects=True)
            if probe.status_code == 200 and "admin" in probe.url:
                crawler_logger.info("登入成功")
                return
        last_err = RuntimeError(f"Login failed at {action_url}")

    if last_err:
        raise last_err
    raise RuntimeError("Login failed for all candidates")

# ---------------------- List Parsing ----------------------
def parse_list_page_for_items(html: str):
    soup = BeautifulSoup(html, "lxml")
    items = []
    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        work_id = None
        cb = tr.select_one("input[type=checkbox][value]")
        if cb:
            work_id = cb.get("value")

        sample_id = ""
        div_long = tr.select_one("td div.small.mb-1")
        if div_long:
            txt = div_long.get_text(strip=True)
            if txt and sum(c.isdigit() for c in txt) >= 11:
                sample_id = txt

        interviewer_no = ""
        interviewer_name = ""
        span = tr.select_one("span.badge.bg-primary")
        if span:
            parts = span.get_text(" ", strip=True).split("/")
            if len(parts) == 2:
                interviewer_no = parts[0].strip()
                interviewer_name = parts[1].strip()

        if work_id:
            items.append({
                "work_id": work_id,
                "sample_id": sample_id,
                "interviewer_no": interviewer_no,
                "interviewer_name": interviewer_name,
            })

    max_page = detect_max_page_from_html(soup)
    return items, max_page


def detect_max_page_from_html(soup: BeautifulSoup) -> int:
    max_page = 1
    for a in soup.select("ul.pagination a.page-link[href]"):
        href = a.get("href", "")
        m = re.search(r"page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page

# ---------------------- Visit Parsing ----------------------
def parse_visits_from_visit_html(html: str) -> List[Dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("div.grid-table table.table")
    if not table:
        return []
    
    visits = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        
        raw_date = tds[0].get_text(strip=True) or ""
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw_date)
        date_txt = m.group(0) if m else ""
        
        session_txt = tds[1].get_text(strip=True)
        
        code_txt = ""
        code_div = tds[2].select_one("div.d-flex > div")
        if code_div:
            code_txt = code_div.get_text(strip=True)
        
        view_url = None
        view_link = tds[3].select_one("a[href*='/form-result/view/']")
        if view_link:
            view_url = view_link.get("href")
        
        log_url = None
        log_link = tds[4].select_one("a[href*='/form-result/logs/']")
        if log_link:
            log_url = log_link.get("href")

        visits.append({
            "date": date_txt,
            "session": session_txt,
            "code": code_txt,
            "view_url": view_url,
            "log_url": log_url,
        })
    
    return visits

# ---------------------- Check Questionnaire Status ----------------------
def check_questionnaire_result_code(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.table.table-bordered")
    
    if len(tables) < 1:
        return "未填寫"
    
    first_table = tables[0]
    
    for tr in first_table.select("tbody tr"):
        tds = tr.find_all(["th", "td"])
        if len(tds) < 2:
            continue
        
        for i in range(len(tds) - 1):
            if tds[i].name == "th" and "結果代碼" in tds[i].get_text(strip=True):
                code = tds[i + 1].get_text(strip=True)
                if code == "100":
                    return "已填寫"
                else:
                    return "未填寫"
    
    return "未填寫"


def check_questionnaires_status(session: requests.Session, work_id: str, project: int, wave: int) -> Dict[str, str]:
    result = {
        "sampling": "未填寫",      # 戶中抽樣
        "sampling_q": "未填寫",    # 戶抽問卷
        "interview_record": "未填寫"  # 訪問記錄問卷
    }
    
    record_url = urljoin(BASE_URL, EDIT_BASE_TMPL.format(project=project, wave=wave, work_id=work_id) + "/record")
    
    try:
        r = session.get(record_url, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return result
        
        soup = BeautifulSoup(r.text, "lxml")
        
        for tr in soup.select("table tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            
            title = tds[1].get_text(strip=True)
            
            link = tds[2].select_one("a[href*='/form-result/view/']")
            
            if link:
                questionnaire_url = urljoin(BASE_URL, link.get("href"))
                
                try:
                    rq = session.get(questionnaire_url, timeout=TIMEOUT, allow_redirects=True)
                    if rq.status_code == 200:
                        html_content = rq.content.decode('utf-8', errors='replace')
                        status = check_questionnaire_result_code(html_content)
                        
                        if "戶中抽樣" in title and "問卷" not in title:
                            result["sampling"] = status
                        elif "戶抽問卷" in title:
                            result["sampling_q"] = status
                        elif "訪問記錄問卷" in title or "訪問記錄" in title:
                            result["interview_record"] = status
                except Exception as e:
                    crawler_logger.debug(f"獲取問卷頁面失敗 {title}: {e}")
        
        return result
    except Exception as e:
        crawler_logger.debug(f"檢查問卷狀態失敗 WorkID={work_id}: {e}")
        return result


def parse_t16_from_visit_survey(html: str, work_id: str = "", debug: bool = False) -> str:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.table.table-bordered")
    
    if len(tables) < 2:
        return "未填寫"
    
    target_table = tables[1]
    
    for tr in target_table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        
        first_col = tds[0].get_text(" ", strip=True)
        
        if "T16" in first_col:
            answer_cell = tds[2]
            
            divs = answer_cell.select("div")
            if divs:
                answers = [div.get_text(strip=True) for div in divs if div.get_text(strip=True)]
                if answers:
                    return "; ".join(answers)
            
            answer_text = answer_cell.get_text(strip=True)
            if answer_text:
                return answer_text
            
            return "未填寫"
    
    return "未填寫"


# ---------------------- Get Visit Survey URL ----------------------
def get_visit_survey_url(session: requests.Session, work_id: str, project: int, wave: int) -> Optional[str]:
    record_url = urljoin(BASE_URL, EDIT_BASE_TMPL.format(project=project, wave=wave, work_id=work_id) + "/record")
    
    try:
        r = session.get(record_url, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        
        soup = BeautifulSoup(r.text, "lxml")
        
        for tr in soup.select("table tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            
            title = tds[1].get_text(strip=True)
            if "TEDS2025_訪視問卷" in title or "訪視問卷" in title:
                link = tds[2].select_one("a[href*='/form-result/view/']")
                if link:
                    return link.get("href")
        
        return None
    except Exception as e:
        crawler_logger.debug(f"獲取訪視問卷 URL 失敗 WorkID={work_id}: {e}")
        return None


def parse_contact_from_view(html: str, work_id: str = "", debug: bool = False) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "lxml")
    
    tables = soup.select("table.table.table-bordered")
    
    target_table = None
    if len(tables) >= 2:
        target_table = tables[1]
    elif len(tables) == 1:
        target_table = tables[0]
    
    if not target_table:
        return ("未填寫", "")

    for idx, tr in enumerate(target_table.select("tbody tr")):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        
        first_col = tds[0].get_text(" ", strip=True)
        
        if "T03" in first_col:
            answer = tds[2].get_text(strip=True)
            answered_at = tds[3].get_text(strip=True)
            
            if not answer:
                answer = "未填寫"
            
            return (answer, answered_at)

    return ("未填寫", "")

# ---------------------- Process Single Item (v2 with debug_work_ids) ----------------------
def process_single_item_v2(
    session: requests.Session, 
    item: Dict, 
    project: int, 
    wave: int, 
    item_idx: int, 
    total: int, 
    debug_work_ids: set,
    update_progress_callback
) -> List[Dict[str, str]]:
    """處理單個樣本（v2 版本）"""
    work_id = item["work_id"]
    sample_id = item["sample_id"]
    interviewer_no = item["interviewer_no"]
    interviewer_name = item["interviewer_name"]
    
    is_debug = work_id in debug_work_ids
    
    rows = []
    
    update_progress_callback(item_idx, total, f"處理樣本: {sample_id} ({work_id})")

    try:
        record_url = urljoin(BASE_URL, EDIT_BASE_TMPL.format(project=project, wave=wave, work_id=work_id) + "/visit")
        r = session.get(record_url, timeout=TIMEOUT, allow_redirects=True)
        
        if r.status_code != 200:
            crawler_logger.warning(f"[{item_idx}/{total}] WorkID={work_id} status={r.status_code}")
            return rows
        
        visits = parse_visits_from_visit_html(r.text)
        
        if not visits:
            rows.append({
                "SampleID": sample_id,
                "WorkID": work_id,
                "Date": "",
                "Session": "無訪次",
                "ResultCode": "",
                "RecordURL": record_url,
                "ViewURL": "",
                "LogsURL": "",
                "InterviewerNo": interviewer_no,
                "InterviewerName": interviewer_name,
                "ContactMethod": "",
                "ContactAnsweredAt": "",
                "T16Answer": "",
                "Sampling": "",
                "SamplingQ": "",
                "InterviewRecord": "",
                "HasFill": "0",
            })
            return rows
        
        for v in visits:
            contact_answer = "未填寫"
            contact_time = ""
            t16_answer = "未填寫"
            has_fill = "0"
            view_url_abs = urljoin(BASE_URL, v["view_url"]) if v.get("view_url") else ""
            
            if view_url_abs:
                has_fill = "1"
                try:
                    rv = session.get(view_url_abs, timeout=TIMEOUT, allow_redirects=True)
                    
                    if rv.status_code == 200:
                        try:
                            html_content = rv.content.decode('utf-8', errors='replace')
                        except Exception:
                            html_content = rv.text
                        
                        ans, ts = parse_contact_from_view(html_content, work_id=work_id, debug=is_debug)
                        contact_answer = ans
                        contact_time = ts
                        
                        if ans != "未填寫":
                            has_fill = "1"
                        else:
                            has_fill = "0"
                except Exception as e:
                    crawler_logger.error(f"View fetch error for WorkID={work_id}: {e}")
                    contact_answer = "未填寫"
                    has_fill = "0"
            elif v.get("log_url"):
                has_fill = "0"
            
            try:
                visit_survey_url = get_visit_survey_url(session, work_id, project, wave)
                if visit_survey_url:
                    visit_url_abs = urljoin(BASE_URL, visit_survey_url)
                    rv_visit = session.get(visit_url_abs, timeout=TIMEOUT, allow_redirects=True)
                    if rv_visit.status_code == 200:
                        visit_html = rv_visit.content.decode('utf-8', errors='replace')
                        t16_answer = parse_t16_from_visit_survey(visit_html, work_id=work_id, debug=is_debug)
            except Exception as e:
                crawler_logger.debug(f"獲取 T16 失敗 WorkID={work_id}: {e}")
            
            questionnaire_status = {"sampling": "未填寫", "sampling_q": "未填寫", "interview_record": "未填寫"}
            
            rows.append({
                "SampleID": sample_id,
                "WorkID": work_id,
                "Date": v.get("date", ""),
                "Session": v.get("session", ""),
                "ResultCode": v.get("code", ""),
                "RecordURL": record_url,
                "ViewURL": view_url_abs,
                "LogsURL": urljoin(BASE_URL, v["log_url"]) if v.get("log_url") else "",
                "InterviewerNo": interviewer_no,
                "InterviewerName": interviewer_name,
                "ContactMethod": contact_answer,
                "ContactAnsweredAt": contact_time,
                "T16Answer": t16_answer,
                "Sampling": questionnaire_status["sampling"],
                "SamplingQ": questionnaire_status["sampling_q"],
                "InterviewRecord": questionnaire_status["interview_record"],
                "HasFill": has_fill,
            })
        
        try:
            questionnaire_status = check_questionnaires_status(session, work_id, project, wave)
            for row in rows:
                if row["WorkID"] == work_id:
                    row["Sampling"] = questionnaire_status["sampling"]
                    row["SamplingQ"] = questionnaire_status["sampling_q"]
                    row["InterviewRecord"] = questionnaire_status["interview_record"]
        except Exception as e:
            crawler_logger.debug(f"檢查問卷狀態失敗 WorkID={work_id}: {e}")
    
    except Exception as e:
        crawler_logger.error(f"[{item_idx}/{total}] WorkID={work_id} error: {e}")
    
    return rows


# ---------------------- Main Crawl (修改為支援 GUI 進度更新) ----------------------
def crawl_from_main_list(session: requests.Session, project: int, wave: int, update_progress_callback, output_dir: Path) -> List[Dict[str, str]]:
    update_progress_callback(0, 100, "1/4: 嘗試登入並獲取清單...")
    
    first_url = urljoin(BASE_URL, LIST_PATH_TMPL.format(project=project, wave=wave, page=1))
    r0 = session.get(first_url, timeout=TIMEOUT, allow_redirects=True)
    r0.raise_for_status()
    
    items, max_page = parse_list_page_for_items(r0.text)
    crawler_logger.info(f"偵測到 {max_page} 個分頁")
    
    if max_page > 1:
        update_progress_callback(10, 100, "1/4: 抓取所有清單頁面...")
        for p in range(2, max_page + 1):
            url = urljoin(BASE_URL, LIST_PATH_TMPL.format(project=project, wave=wave, page=p))
            r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
            items_p, _ = parse_list_page_for_items(r.text)
            items.extend(items_p)
    
    crawler_logger.info(f"總計 {len(items)} 筆樣本")
    
    update_progress_callback(20, 100, f"2/4: 預處理前 500 筆以找出 DEBUG 目標...")
    
    debug_work_ids = set()
    for item in items[:500]:
        if len(debug_work_ids) >= 5:
            break
        worker_session = create_session()
        worker_session.cookies.update(session.cookies)
        record_url = urljoin(BASE_URL, EDIT_BASE_TMPL.format(project=project, wave=wave, work_id=item["work_id"]) + "/visit")
        try:
            r = worker_session.get(record_url, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                visits = parse_visits_from_visit_html(r.text)
                if visits and any(v.get("view_url") for v in visits):
                    debug_work_ids.add(item["work_id"])
                    crawler_logger.info(f"找到有 ViewURL 的 WorkID: {item['work_id']}")
        except Exception as e:
            crawler_logger.debug(f"預處理 {item['work_id']} 失敗: {e}")
    
    update_progress_callback(25, 100, "3/4: 開始並行處理樣本...")
    
    all_rows: List[Dict[str, str]] = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for idx, item in enumerate(items, 1):
            worker_session = create_session()
            worker_session.cookies.update(session.cookies)
            future = executor.submit(
                process_single_item_v2, 
                worker_session, item, project, wave, idx, len(items), debug_work_ids, 
                lambda current, total, message: update_progress_callback(
                    25 + int(70 * current / total), 100, 
                    f"3/4: ({current}/{total}) {message}"
                )
            )
            futures[future] = item["work_id"]
        
        completed = 0
        for future in as_completed(futures):
            work_id = futures[future]
            try:
                rows = future.result()
                all_rows.extend(rows)
                completed += 1
                
                update_progress_callback(
                    25 + int(70 * completed / len(items)), 100, 
                    f"3/4: ({completed}/{len(items)}) 樣本 {work_id} 完成"
                )
            except Exception as e:
                crawler_logger.error(f"處理 WorkID={work_id} 時發生錯誤: {e}")
    
    update_progress_callback(95, 100, f"4/4: 爬取完成，總計 {len(all_rows)} 筆訪次記錄。")
    return all_rows

# ---------------------- CSV Output ----------------------
def write_csv(rows: List[Dict[str, str]], path: str) -> str:
    if not rows:
        crawler_logger.info("無資料可寫出")
        return ""
    
    fieldnames = [
        "SampleID", "WorkID", "Date", "Session", "ResultCode", "RecordURL",
        "ViewURL", "LogsURL", "InterviewerNo", "InterviewerName",
        "ContactMethod", "ContactAnsweredAt", "T16Answer",
        "Sampling", "SamplingQ", "InterviewRecord", "HasFill",
    ]
    
    for row in rows:
        for field in fieldnames:
            if field not in row:
                row[field] = ""
    
    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        crawler_logger.info(f"輸出 {len(rows)} 列到 {path}")
        return path
    except Exception as e:
        crawler_logger.error(f"寫出 CSV 失敗: {e}")
        return ""

# =================================================================
# 核心功能區塊 - 檢查邏輯
# =================================================================

def norm(s) -> str:
    return "" if s is None else str(s).strip()

def is_filled(v: str) -> bool:
    s = norm(v)
    if s == "":
        return False
    if s in {"未填寫", "未填", "NA", "N/A", "None", "null"}:
        return False
    return True

def normalize_result_code(code: str):
    s = norm(code)
    if not s:
        return ""
    m = re.match(r"(\d+)\.0+$", s)
    if m:
        return m.group(1)
    return s

def parse_datetime(dt_str: str) -> pd.Timestamp:
    s = norm(dt_str)
    if s == "":
        return pd.NaT
    fmts = ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"]
    for f in fmts:
        try:
            return pd.to_datetime(s, format=f)
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce")

def session_bucket(session: str) -> str:
    s = norm(session)
    if any(k in s for k in ["白天", "上午", "早上", "日間", "白日"]):
        return "白天"
    if "下午" in s:
        return "下午"
    if any(k in s for k in ["晚上", "夜間", "夜晚"]):
        return "晚上"
    su = s.upper()
    if su in {"D", "DAY"}:
        return "白天"
    if su in {"A", "AFTERNOON"}:
        return "下午"
    if su in {"E", "EVENING", "NIGHT"}:
        return "晚上"
    return "未知"

def load_holidays(path: str) -> Set[pd.Timestamp]:
    days: Set[pd.Timestamp] = set()
    if not path:
        return days
    p = Path(path)
    if not p.exists():
        return days
    try:
        with p.open("r", encoding="utf-8") as fh:
            for line in fh:
                d = line.strip()
                if not d:
                    continue
                try:
                    days.add(pd.to_datetime(d).normalize())
                except Exception:
                    pass
    except Exception as e:
        crawler_logger.error(f"讀取假日清單失敗: {e}")
    return days

def is_weekend_or_holiday(ts: pd.Timestamp, holidays: Set[pd.Timestamp]) -> bool:
    if pd.isna(ts):
        return False
    if ts.weekday() >= 5:
        return True
    return ts.normalize() in holidays

def extract_t16_numbers(t16: str) -> Set[str]:
    s = norm(t16)
    nums = set(re.findall(r"(\d+)\s*:", s))
    return nums

def contact_is_guard(contact: str) -> bool:
    return "警衛" in norm(contact)

def contact_is_public_servant(contact: str) -> bool:
    s = norm(contact)
    return any(k in s for k in ["鄰里長", "員警", "警察", "郵差", "公職人員", "警衛"]) or "里長" in s

def check_I_three_visits(df: pd.DataFrame) -> List[Dict]:
    recs: List[Dict] = []
    df_sorted = df.sort_values(["SampleID", "DateTime", "_row"], kind="mergesort")

    for sid, g in df_sorted.groupby("SampleID", sort=False):
        last = g.iloc[-1]
        last_code = norm(last["ResultCode3"])
        if not last_code.startswith("2"):
            continue

        total_visits = len(g)
        missing_visits = max(0, 3 - total_visits)

        sessions_present = set([s for s in g["SessionBucket"].unique() if s in {"白天", "下午", "晚上"}])
        missing_sessions = [s for s in ["白天", "下午", "晚上"] if s not in sessions_present]

        holiday_visits = int(g["IsWeekendOrHoliday"].sum())
        missing_holiday = 1 - min(1, holiday_visits)

        if (missing_visits > 0) or (len(sessions_present) < 2) or (missing_holiday > 0):
            interviewer = g["InterviewerName"].mode().iat[0] if not g["InterviewerName"].mode().empty else ""
            issue = f"【三訪規則】缺少訪次數:{missing_visits}；缺少假日/週末訪次:{missing_holiday}；已涵蓋時段:{('、'.join(sorted(list(sessions_present))) if sessions_present else '無')}；缺少時段:{('、'.join(missing_sessions) if missing_sessions else '無')}"
            recs.append({
                "樣本編號": sid,
                "訪員姓名": interviewer,
                "日期": "",
                "結果代碼": last_code,
                "問題描述": issue,
                "檢查類別": "I.三訪規則"
            })

    return recs


def check_II_questionnaire(df: pd.DataFrame) -> List[Dict]:
    forbidden = {"202","206","207","302","303","304","311","312","313","324","329"}
    allowed_newer = {"201","203","204","205","301","305","306","307","309","310","311","314","315","316","317","318","319","320","321","322","325","326","331","100"}
    must_have_sampling = {"201","203","204","205","301","305","306","307","309","310","311","314","315","316","317","318","319","320","321","322","325","326","331"}

    recs: List[Dict] = []

    def push(row, msg, category):
        recs.append({
            "樣本編號": row["SampleID"],
            "訪員姓名": row["InterviewerName"],
            "日期": row["Date"],
            "結果代碼": row["ResultCode"],
            "問題描述": msg,
            "檢查類別": category,
        })

    g = df.sort_values(["SampleID", "DateTime", "_row"], kind="mergesort").reset_index(drop=True)

    sample_has_100 = g.groupby("SampleID")["ResultCode3"].apply(lambda x: "100" in x.values).to_dict()
    g["SampleHas100"] = g["SampleID"].map(sample_has_100)

    has_future_allowed = [False] * len(g)
    prev_sid = None
    future_flag = False
    for i in range(len(g) - 1, -1, -1):
        row = g.iloc[i]
        sid = row["SampleID"]
        if sid != prev_sid:
            future_flag = False
            prev_sid = sid
        has_future_allowed[i] = future_flag
        if row["ResultCode3"] in allowed_newer:
            future_flag = True
    g["HasFutureAllowed"] = has_future_allowed

    for _, row in g.iterrows():
        code3 = norm(row["ResultCode3"])
        has_rc = is_filled(row["ResultCode"])

        if not has_rc:
            if row["T16Filled"] or row["SamplingFilled"] or row["SamplingQFilled"] or row["InterviewRecordFilled"]:
                push(row, "【問卷填寫】無結果代碼卻出現訪視問卷/戶抽/戶抽問卷/訪問記錄問卷", "II.問卷填寫")
            continue

        if not row["T16Filled"]:
            push(row, "【問卷填寫】訪視問卷未填", "II.問卷填寫")

        if code3 in forbidden:
            if row["SamplingFilled"] or row["SamplingQFilled"] or row["InterviewRecordFilled"]:
                if not row["HasFutureAllowed"] and not row["SampleHas100"]:
                    push(row, "【問卷填寫】此結果代碼不應有戶抽/填戶抽問卷/訪問記錄問卷，請重新檢查", "II.問卷填寫")
                    
        if code3 in must_have_sampling:
            if not (row["SamplingFilled"] and row["SamplingQFilled"]):
                push(row, "【問卷填寫】此代碼需戶抽與填戶抽問卷", "II.問卷填寫")

    for sid, grp in g.groupby("SampleID", sort=False):
        last = grp.iloc[-1]
        if norm(last["ResultCode3"]) == "100":
            if not (last["T16Filled"] and last["SamplingFilled"] and last["SamplingQFilled"] and last["InterviewRecordFilled"]):
                recs.append({
                    "樣本編號": last["SampleID"],
                    "訪員姓名": last["InterviewerName"],
                    "日期": last["Date"],
                    "結果代碼": last["ResultCode"],
                    "問題描述": "【問卷填寫】為成功樣本，但有資料未填寫完成",
                    "檢查類別": "II.問卷填寫",
                })

    return recs


def check_III_content(df: pd.DataFrame) -> List[Dict]:
    recs: List[Dict] = []

    def push(row, msg):
        recs.append({
            "樣本編號": row["SampleID"],
            "訪員姓名": row["InterviewerName"],
            "日期": row["Date"],
            "結果代碼": row["ResultCode"],
            "問題描述": msg,
            "檢查類別": "III.問卷內容",
        })

    for _, row in df.iterrows():
        code3 = norm(row["ResultCode3"]) or ""
        contact = norm(row["ContactMethod"]) or ""
        t16 = norm(row["T16Answer"]) or ""
        t16_nums = extract_t16_numbers(t16)

        if contact_is_guard(contact):
            if ("3" not in t16_nums) and ("警衛" not in t16):
                push(row, "【問卷內容】接觸方式為警衛，但訪視問卷未包含『警衛或管理員』")

        if "對講機" in contact:
            if ("2" not in t16_nums) and ("對講機" not in t16):
                push(row, "【問卷內容】接觸方式為對講機，但訪視問卷未包含『對講機』")

        if code3 == "304":
            if not contact_is_guard(contact):
                push(row, "【問卷內容】結果代碼為304，但接觸方式並非『警衛』")

        if code3 in {"311", "312"}:
            if not contact_is_public_servant(contact):
                push(row, "【問卷內容】結果代碼為311或312，但接觸方式非公職人員（鄰里長/員警/郵差等）")

    return recs


def check_IV_latest_codes(df: pd.DataFrame) -> List[Dict]:
    target = {"305","314","315","316","317","318","319","320","321","322","323","324","326","329","330","331"}
    recs: List[Dict] = []

    df_sorted = df.sort_values(["SampleID", "DateTime", "_row"], kind="mergesort")
    for sid, grp in df_sorted.groupby("SampleID", sort=False):
        last = grp.iloc[-1]
        code3 = norm(last["ResultCode3"]) or ""
        if code3 in target:
            recs.append({
                "樣本編號": last["SampleID"],
                "訪員姓名": last["InterviewerName"],
                "日期": last["Date"],
                "結果代碼": last["ResultCode"],
                "問題描述": f"【訪次檢查】訪次結果代碼={code3}，請說明接觸情形",
                "檢查類別": "IV.訪次檢查",
            })

    return recs


def run_all_checks(csv_path: str, holidays_path: str, output_dir: Path, update_progress_callback) -> Tuple[bool, int]:
    update_progress_callback(96, 100, "4/4: 讀取資料並準備檢查...")
    try:
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", na_filter=False)
    except Exception as e:
        messagebox.showerror("錯誤", f"讀取爬蟲結果 CSV 失敗：{e}")
        return False, 0
    
    df.columns = [c.strip() for c in df.columns]

    df = df.copy()
    df["_row"] = range(len(df))
    df["ResultCode3"] = df["ResultCode"].apply(normalize_result_code)
    df["DateTime"] = df["Date"].apply(parse_datetime)
    df["SessionBucket"] = df["Session"].apply(session_bucket)

    holidays = load_holidays(holidays_path)
    df["IsWeekendOrHoliday"] = df["DateTime"].apply(lambda x: is_weekend_or_holiday(x, holidays))

    df["T16Filled"] = df["T16Answer"].apply(is_filled)
    df["SamplingFilled"] = df["Sampling"].apply(is_filled)
    df["SamplingQFilled"] = df["SamplingQ"].apply(is_filled)
    df["InterviewRecordFilled"] = df["InterviewRecord"].apply(is_filled)

    update_progress_callback(97, 100, "4/4: 執行邏輯一致性檢查...")
    all_issues = []
    all_issues.extend(check_I_three_visits(df))
    all_issues.extend(check_II_questionnaire(df))
    all_issues.extend(check_III_content(df))
    all_issues.extend(check_IV_latest_codes(df))

    issues_df = pd.DataFrame(all_issues)
    
    if len(issues_df) == 0:
        crawler_logger.info("恭喜！沒有發現任何問題。")
        summary = pd.DataFrame([{"訪員姓名": "全部", "違規總數": 0}])
        summary_path = output_dir / "check_summary_by_interviewer.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        return True, 0

    update_progress_callback(98, 100, "4/4: 輸出違規清單檔案...")
    for interviewer, grp in issues_df.groupby("訪員姓名"):
        safe_name = re.sub(r'[\\/:*?"<>|]', '_', str(interviewer))
        filename = f"interviewer_{safe_name}.csv"
        grp_sorted = grp.sort_values(["樣本編號", "日期"])
        grp_sorted[["樣本編號", "日期", "結果代碼", "問題描述", "檢查類別"]].to_csv(
            output_dir / filename, index=False, encoding="utf-8-sig"
        )
        crawler_logger.info(f"已輸出：{filename} ({len(grp)} 筆問題)")

    summary = issues_df.groupby("訪員姓名").size().reset_index(name="違規總數")
    summary = summary.sort_values("違規總數", ascending=False)
    summary_path = output_dir / "check_summary_by_interviewer.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    
    total_issues = len(issues_df)
    crawler_logger.info(f"\n完成！共發現 {total_issues} 個問題，涉及 {len(summary)} 位訪員")
    return True, total_issues


# =================================================================
# GUI 區塊 (使用 CustomTkinter) - UI 終極美化版 v2.3
# =================================================================

# 設定 CustomTkinter 預設主題
ctk.set_appearance_mode("System")  # 預設為系統主題
ctk.set_default_color_theme("blue")  # 使用藍色主題

class VisitCrawlerApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("訪次資料匯出檢查 | By.莊旻叡")
        self.geometry("780x650")
        self.resizable(False, False) 

        # 狀態變數
        self.email_var = ctk.StringVar(value="")
        self.password_var = ctk.StringVar()
        self.project_var = ctk.StringVar(value="35")
        self.wave_var = ctk.StringVar(value="99")
        self.holiday_path_var = ctk.StringVar(value="未選擇")
        self._full_holiday_path: Optional[Path] = None
        self.output_dir = Path.cwd() / "Output"
        
        # 顏色常量 (CTk 會自動處理深淺模式)
        self.ACCENT_COLOR = "#1F4E79" # Dark Navy/Blue
        self.FONT_FAMILY = "微軟正黑體"
        
        self._create_widgets()
        self.bind('<Return>', lambda e: self._start_crawl_thread())
        
    def _create_widgets(self):
        # 主容器框架 (使用 CTkFrame，padding 與圓角效果)
        main_frame = ctk.CTkFrame(self, fg_color="transparent")
        main_frame.pack(fill="both", expand=True, padx=30, pady=30)

        # 頂部標題
        ctk.CTkLabel(main_frame, text="訪次資料匯出檢查", 
                     font=(self.FONT_FAMILY, 24, 'bold'),
                     text_color=self.ACCENT_COLOR).pack(pady=(0, 25))

        # --- 1. 執行參數框架 ---
        # 使用 CTkFrame 模擬 LabelFrame，視覺上更簡潔
        input_frame = ctk.CTkFrame(main_frame, corner_radius=10)
        input_frame.pack(padx=0, pady=(0, 25), fill="x", ipady=15)

        # 網格配置
        input_frame.columnconfigure(0, weight=1, minsize=160) 
        input_frame.columnconfigure(1, weight=3) 

        row_index = 0
        pady_val = 10
        padx_val = 15
        
        # A. 登入憑證 (分組標題)
        ctk.CTkLabel(input_frame, text="[ 登入憑證 ]", 
                     font=(self.FONT_FAMILY, 15, 'bold'), 
                     text_color=ctk.ThemeManager.theme["CTkButton"]["fg_color"][0], # 使用主題色
                     ).grid(row=row_index, column=0, sticky="w", pady=(pady_val, 0), padx=padx_val, columnspan=2)
        row_index += 1
        
        # 帳號 (Email)
        ctk.CTkLabel(input_frame, text="帳號 (Email):", font=(self.FONT_FAMILY, 13, 'bold')).grid(row=row_index, column=0, sticky="w", pady=pady_val, padx=padx_val)
        ctk.CTkEntry(input_frame, textvariable=self.email_var, font=(self.FONT_FAMILY, 13)).grid(row=row_index, column=1, sticky="ew", pady=pady_val, padx=padx_val)
        row_index += 1

        # 密碼
        ctk.CTkLabel(input_frame, text="密碼:", font=(self.FONT_FAMILY, 13, 'bold')).grid(row=row_index, column=0, sticky="w", pady=pady_val, padx=padx_val)
        ctk.CTkEntry(input_frame, textvariable=self.password_var, show="•", font=(self.FONT_FAMILY, 13)).grid(row=row_index, column=1, sticky="ew", pady=pady_val, padx=padx_val)
        row_index += 1

        # B. 專案配置 (分組標題)
        ctk.CTkLabel(input_frame, text="[ 專案配置 ]", 
                     font=(self.FONT_FAMILY, 15, 'bold'), 
                     text_color=ctk.ThemeManager.theme["CTkButton"]["fg_color"][0],
                     ).grid(row=row_index, column=0, sticky="w", pady=(pady_val*2, 0), padx=padx_val, columnspan=2)
        row_index += 1
        
        # Project ID / Wave ID 容器
        project_wave_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        project_wave_frame.grid(row=row_index, column=1, sticky="ew", pady=pady_val, padx=padx_val)
        project_wave_frame.columnconfigure(0, weight=1) 
        project_wave_frame.columnconfigure(2, weight=0) # 分隔符不佔空間
        project_wave_frame.columnconfigure(3, weight=1) 

        ctk.CTkLabel(input_frame, text="Project ID / Wave ID:", font=(self.FONT_FAMILY, 13, 'bold')).grid(row=row_index, column=0, sticky="w", pady=pady_val, padx=padx_val)
        
        ctk.CTkEntry(project_wave_frame, textvariable=self.project_var, font=(self.FONT_FAMILY, 13)).grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(project_wave_frame, text=" / ", font=(self.FONT_FAMILY, 13, 'bold')).grid(row=0, column=2, sticky="ew", padx=10)
        ctk.CTkEntry(project_wave_frame, textvariable=self.wave_var, font=(self.FONT_FAMILY, 13)).grid(row=0, column=3, sticky="ew")
        row_index += 1
        
        # 假日清單按鈕 (優化 UX)
        holiday_control_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        holiday_control_frame.grid(row=row_index, column=1, sticky="ew", pady=pady_val, padx=padx_val)
        holiday_control_frame.columnconfigure(0, weight=0) 
        holiday_control_frame.columnconfigure(1, weight=1) 

        ctk.CTkLabel(input_frame, text="國定假日清單 (選填):", font=(self.FONT_FAMILY, 13, 'bold')).grid(row=row_index, column=0, sticky="w", pady=pady_val, padx=padx_val)
        
        ctk.CTkButton(holiday_control_frame, text="選擇檔案...", command=self._select_holiday_file, 
                      width=150, font=(self.FONT_FAMILY, 12),
                      fg_color=("gray70", "gray35") # 次要按鈕樣式
                      ).grid(row=0, column=0, sticky="w")
        
        # 顯示選中的檔案名稱
        self.holiday_path_display = ctk.CTkLabel(holiday_control_frame, textvariable=self.holiday_path_var, wraplength=400, 
                                                 font=(self.FONT_FAMILY, 11), text_color=("gray40", "gray60"))
        self.holiday_path_display.grid(row=0, column=1, sticky="w", padx=(10, 0))
        row_index += 1

        # --- 2. 執行按鈕 ---
        self.run_button = ctk.CTkButton(main_frame, text="▶ 啟動爬取與檢查", command=self._start_crawl_thread, 
                                        height=50, 
                                        font=(self.FONT_FAMILY, 16, 'bold'))
        self.run_button.pack(pady=(20, 30), fill="x")

        # --- 3. 執行進度框架 ---
        progress_frame = ctk.CTkFrame(main_frame, corner_radius=10)
        progress_frame.pack(padx=0, pady=(0, 25), fill="x", ipady=15)
        
        # 狀態標籤 
        self.status_label = ctk.CTkLabel(progress_frame, text="系統待命中...", anchor="center", 
                                         font=(self.FONT_FAMILY, 15, 'bold'), 
                                         text_color=self.ACCENT_COLOR)
        self.status_label.pack(pady=(5, 15), fill="x")
        
        # 進度條
        self.progress = ctk.CTkProgressBar(progress_frame, orientation="horizontal", height=20)
        self.progress.set(0)
        self.progress.pack(pady=5, padx=15, fill="x")

        # --- 4. 資訊區與模式切換 ---
        info_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        info_frame.pack(padx=0, pady=(0, 0), fill="x")
        info_frame.columnconfigure(0, weight=1)
        info_frame.columnconfigure(1, weight=1)

        # 輸出路徑
        self.output_label = ctk.CTkLabel(info_frame, text=f"輸出資料夾: {self.output_dir.name}", 
                                         font=(self.FONT_FAMILY, 11), 
                                         anchor="w", text_color=self.ACCENT_COLOR)
        self.output_label.grid(row=0, column=0, sticky="w")
        
        # 主題切換按鈕
        self.appearance_mode_optionemenu = ctk.CTkOptionMenu(info_frame, 
                                                             values=["Light", "Dark", "System"],
                                                             command=self.change_appearance_mode_event,
                                                             width=100,
                                                             font=(self.FONT_FAMILY, 11))
        self.appearance_mode_optionemenu.set("System")
        self.appearance_mode_optionemenu.grid(row=0, column=1, sticky="e")
        
        # 作者資訊
        ctk.CTkLabel(info_frame, text="By.莊旻叡", 
                     font=(self.FONT_FAMILY, 9), text_color=("gray60", "gray40")).grid(row=1, column=0, sticky="w")

    def change_appearance_mode_event(self, new_appearance_mode: str):
        ctk.set_appearance_mode(new_appearance_mode)

    def _select_holiday_file(self):
        file_path = filedialog.askopenfilename(
            title="選擇國定假日清單 (.txt)",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if file_path:
            self._full_holiday_path = Path(file_path)
            self.holiday_path_var.set(self._full_holiday_path.name) 
        else:
            self.holiday_path_var.set("未選擇")
            self._full_holiday_path = None

    def _update_progress(self, current, total, message):
        percentage = max(0, min(100, (current / total) * 100))
        
        # CTkProgressBar 使用 set(value)
        self.progress.set(percentage / 100) 
        self.status_label.configure(text=f"{message} ({percentage:.1f}%)")
        self.update_idletasks()
        
    def _start_crawl_thread(self):
        email = self.email_var.get().strip()
        password = self.password_var.get().strip()
        project_id = self.project_var.get().strip()
        wave_id = self.wave_var.get().strip()
        
        holiday_path = str(self._full_holiday_path) if self._full_holiday_path else ""

        if not email or "@" not in email:
            messagebox.showerror("驗證錯誤", "請輸入有效的 Email 帳號。")
            return
        if not password:
            messagebox.showerror("驗證錯誤", "請輸入密碼。")
            return
        if not project_id.isdigit() or not wave_id.isdigit():
            messagebox.showerror("驗證錯誤", "Project ID 和 Wave ID 必須是數字。")
            return
            
        self.run_button.configure(state="disabled")
        self.progress.set(0)
        self.status_label.configure(text="初始化...")
        
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            output_csv = str(self.output_dir / "visit_records.csv")
        except Exception as e:
            messagebox.showerror("錯誤", f"無法建立輸出目錄: {e}")
            self.run_button.configure(state="normal")
            return
            
        threading.Thread(
            target=self._run_crawl_and_check, 
            args=(email, password, int(project_id), int(wave_id), output_csv, holiday_path),
            daemon=True
        ).start()

    def _run_crawl_and_check(self, email, password, project, wave, output_csv, holiday_path):
        total_issues = 0
        try:
            session = create_session()
            
            # 1. 登入
            self._update_progress(1, 100, "1/4: 嘗試登入...")
            fetch_csrf_and_login(session, email, password)
            
            # 2. 爬取
            records = crawl_from_main_list(session, project, wave, self._update_progress, self.output_dir)
            
            # 3. 寫出 CSV
            self._update_progress(95, 100, "4/4: 寫出訪次記錄 CSV...")
            csv_path = write_csv(records, output_csv)
            
            if not csv_path:
                raise RuntimeError("無法寫出訪次記錄 CSV。")
            
            # 4. 執行檢查
            success, total_issues = run_all_checks(csv_path, holiday_path, self.output_dir, self._update_progress)
            
            # 5. 完成
            self._update_progress(100, 100, "✅ 完成所有任務！")
            
            if success:
                messagebox.showinfo(
                    "完成", 
                    f"資料匯出與檢查成功！\n\n檔案已輸出至：{self.output_dir.name} 資料夾\n\n共發現 {total_issues} 個問題。\n\n本程式由莊旻叡撰寫\n特別感謝陳逸龍教授加博士先生的協助開發"
                )
            else:
                messagebox.showwarning("警告", f"資料爬取與檢查成功，但檢查過程中發生錯誤或未生成彙總檔案。\n輸出路徑：{self.output_dir.name}")

        except requests.exceptions.HTTPError as e:
            messagebox.showerror("錯誤", f"HTTP 錯誤: 檢查您的 Project/Wave ID 或登入狀態。\n錯誤細節: {e}")
            self._update_progress(0, 100, "❌ 錯誤：HTTP 失敗。")
        except RuntimeError as e:
            messagebox.showerror("錯誤", f"執行錯誤: {e}")
            self._update_progress(0, 100, "❌ 錯誤：登入或執行失敗。")
        except Exception as e:
            messagebox.showerror("嚴重錯誤", f"發生無法預期的錯誤: {e}")
            self._update_progress(0, 100, "❌ 錯誤：執行失敗。")
        finally:
            self.run_button.configure(state="normal")


if __name__ == "__main__":
    try:
        log_dir = Path.cwd() / "logs"
        log_dir.mkdir(exist_ok=True)
        if not any(isinstance(handler, logging.FileHandler) for handler in crawler_logger.handlers):
            file_handler = logging.FileHandler(log_dir / 'crawler_log.txt', encoding='utf-8')
            file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            crawler_logger.addHandler(file_handler)
    except Exception as e:
        print(f"無法設定日誌檔案: {e}")
        
    app = VisitCrawlerApp()
    app.mainloop()
