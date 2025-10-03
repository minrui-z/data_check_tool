import tkinter as tk
from tkinter import ttk, messagebox, filedialog
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

# ---------------------- Basic Config (部分改為從 GUI 傳入) ----------------------
BASE_URL = "https://esccapi.nccu.edu.tw"
LIST_PATH_TMPL = "/admin/project/{project}/wave/{wave}/survey-work/list?page={page}"
EDIT_BASE_TMPL = "/admin/project/{project}/wave/{wave}/survey-work/edit/{work_id}"

# OUTPUT_CSV, DEBUG_DIR 將從 GUI 的執行函式中決定

# 並行設定
MAX_WORKERS = 15
TIMEOUT = 15

# 爬蟲的日誌器
crawler_logger = logging.getLogger("Crawler")
crawler_logger.setLevel(logging.INFO)
# 避免重複設定 Handler
if not crawler_logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch.setFormatter(formatter)
    crawler_logger.addHandler(ch)

# ---------------------- Session Factory ----------------------
def create_session() -> requests.Session:
    # 邏輯與 v6.0.1 相同
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
    # 邏輯與 v6.0.1 相同
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
    # 邏輯與 v6.0.1 相同
    max_page = 1
    for a in soup.select("ul.pagination a.page-link[href]"):
        href = a.get("href", "")
        m = re.search(r"page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page

# ---------------------- Visit Parsing (修正版) ----------------------
def parse_visits_from_visit_html(html: str) -> List[Dict[str, Optional[str]]]:
    # 邏輯與 v6.0.1 相同
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("div.grid-table table.table")
    if not table:
        return []
    
    visits = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:  # 至少要有5欄
            continue
        
        # 第1欄：日期
        raw_date = tds[0].get_text(strip=True) or ""
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw_date)
        date_txt = m.group(0) if m else ""
        
        # 第2欄：時段（直接文字）
        session_txt = tds[1].get_text(strip=True)
        
        # 第3欄：結果代碼（在 div 內）
        code_txt = ""
        code_div = tds[2].select_one("div.d-flex > div")
        if code_div:
            code_txt = code_div.get_text(strip=True)
        
        # 第4欄：觀看提交連結
        view_url = None
        view_link = tds[3].select_one("a[href*='/form-result/view/']")
        if view_link:
            view_url = view_link.get("href")
        
        # 第5欄：填答記錄連結
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
    # 邏輯與 v6.0.1 相同
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.table.table-bordered")
    
    if len(tables) < 1:
        return "未填寫"
    
    # 第一個表格是樣本資訊表
    first_table = tables[0]
    
    for tr in first_table.select("tbody tr"):
        tds = tr.find_all(["th", "td"])
        if len(tds) < 2:
            continue
        
        # 找到「結果代碼」那一列
        for i in range(len(tds) - 1):
            if tds[i].name == "th" and "結果代碼" in tds[i].get_text(strip=True):
                code = tds[i + 1].get_text(strip=True)
                if code == "100":
                    return "已填寫"
                else:
                    return "未填寫"
    
    return "未填寫"


def check_questionnaires_status(session: requests.Session, work_id: str, project: int, wave: int) -> Dict[str, str]:
    # 邏輯與 v6.0.1 相同
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
        
        # 遍歷所有問卷列
        for tr in soup.select("table tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            
            # 第2欄是問卷標題
            title = tds[1].get_text(strip=True)
            
            # 第3欄是「問卷結果」連結
            link = tds[2].select_one("a[href*='/form-result/view/']")
            
            if link:
                questionnaire_url = urljoin(BASE_URL, link.get("href"))
                
                # 抓取問卷頁面並檢查結果代碼
                try:
                    rq = session.get(questionnaire_url, timeout=TIMEOUT, allow_redirects=True)
                    if rq.status_code == 200:
                        html_content = rq.content.decode('utf-8', errors='replace')
                        status = check_questionnaire_result_code(html_content)
                        
                        # 戶中抽樣
                        if "戶中抽樣" in title and "問卷" not in title:
                            result["sampling"] = status
                        # 戶抽問卷
                        elif "戶抽問卷" in title:
                            result["sampling_q"] = status
                        # 訪問記錄問卷
                        elif "訪問記錄問卷" in title or "訪問記錄" in title:
                            result["interview_record"] = status
                except Exception as e:
                    crawler_logger.debug(f"獲取問卷頁面失敗 {title}: {e}")
        
        return result
    except Exception as e:
        crawler_logger.debug(f"檢查問卷狀態失敗 WorkID={work_id}: {e}")
        return result


def parse_t16_from_visit_survey(html: str, work_id: str = "", debug: bool = False) -> str:
    # 邏輯與 v6.0.1 相同
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.table.table-bordered")
    
    if len(tables) < 2:
        return "未填寫"
    
    target_table = tables[1]  # 第2個表格是問卷內容
    
    for tr in target_table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        
        first_col = tds[0].get_text(" ", strip=True)
        
        if "T16" in first_col:
            # 複選題的答案可能在多個 div 裡，或者是純文字
            answer_cell = tds[2]
            
            # 先嘗試抓 div（複選題每個選項一個 div）
            divs = answer_cell.select("div")
            if divs:
                answers = [div.get_text(strip=True) for div in divs if div.get_text(strip=True)]
                if answers:
                    return "; ".join(answers)
            
            # 如果沒有 div，直接抓文字
            answer_text = answer_cell.get_text(strip=True)
            if answer_text:
                return answer_text
            
            return "未填寫"
    
    return "未填寫"


# ---------------------- Get Visit Survey URL ----------------------
def get_visit_survey_url(session: requests.Session, work_id: str, project: int, wave: int) -> Optional[str]:
    # 邏輯與 v6.0.1 相同
    record_url = urljoin(BASE_URL, EDIT_BASE_TMPL.format(project=project, wave=wave, work_id=work_id) + "/record")
    
    try:
        r = session.get(record_url, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        
        soup = BeautifulSoup(r.text, "lxml")
        
        # 找到包含「TEDS2025_訪視問卷」的列
        for tr in soup.select("table tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            
            # 第2欄是問卷標題
            title = tds[1].get_text(strip=True)
            if "TEDS2025_訪視問卷" in title or "訪視問卷" in title:
                # 第3欄是「問卷結果」按鈕
                link = tds[2].select_one("a[href*='/form-result/view/']")
                if link:
                    return link.get("href")
        
        return None
    except Exception as e:
        crawler_logger.debug(f"獲取訪視問卷 URL 失敗 WorkID={work_id}: {e}")
        return None


def parse_contact_from_view(html: str, work_id: str = "", debug: bool = False) -> Tuple[str, str]:
    # 邏輯與 v6.0.1 相同
    soup = BeautifulSoup(html, "lxml")
    
    # 取得所有 table.table.table-bordered 表格
    tables = soup.select("table.table.table-bordered")
    
    if debug and work_id:
        crawler_logger.info(f"WorkID={work_id}: 找到 {len(tables)} 個表格")
    
    # 通常第1個是樣本資訊表，第2個才是問卷內容表
    target_table = None
    if len(tables) >= 2:
        target_table = tables[1]  # 第2個表格
    elif len(tables) == 1:
        target_table = tables[0]  # 只有1個就用它
    
    if not target_table:
        if debug and work_id:
            crawler_logger.error(f"WorkID={work_id}: 完全找不到任何表格！")
        return ("未填寫", "")

    for idx, tr in enumerate(target_table.select("tbody tr")):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        
        # 檢查第1欄是否包含 T03
        first_col = tds[0].get_text(" ", strip=True)
        
        if "T03" in first_col:
            answer = tds[2].get_text(strip=True)      # 第3欄：完整答案
            answered_at = tds[3].get_text(strip=True)  # 第4欄：填答時間
            
            if debug and work_id:
                crawler_logger.info(f"WorkID={work_id}: 找到 T03！答案='{answer}', 時間='{answered_at}'")
            
            # 如果答案為空，標記為未填寫
            if not answer:
                answer = "未填寫"
            
            return (answer, answered_at)

    # 沒有找到 T03 題目
    if debug and work_id:
        crawler_logger.warning(f"WorkID={work_id}: 遍歷完所有列，沒有找到 T03")
    return ("未填寫", "")

# ---------------------- Process Single Item (v2 with debug_work_ids) ----------------------
# 為了進度條，將此函數獨立，並傳入進度更新的回呼函式
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
    
    # 每次處理前更新進度
    update_progress_callback(item_idx, total, f"處理樣本: {sample_id} ({work_id})")

    try:
        # ... (中間的邏輯與 v6.0.1.py 相同) ...
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
            t16_answer = "未填寫"  # 新增 T16
            has_fill = "0"
            view_url_abs = urljoin(BASE_URL, v["view_url"]) if v.get("view_url") else ""
            
            if view_url_abs:
                has_fill = "1"
                try:
                    if is_debug:
                        crawler_logger.info(f"[DEBUG] WorkID={work_id} 準備抓取 view_url: {view_url_abs}")
                    
                    rv = session.get(view_url_abs, timeout=TIMEOUT, allow_redirects=True)
                    
                    if is_debug:
                        crawler_logger.info(f"[DEBUG] WorkID={work_id} GET status={rv.status_code}, encoding={rv.encoding}")
                    
                    if rv.status_code == 200:
                        try:
                            html_content = rv.content.decode('utf-8', errors='replace')
                        except Exception:
                            html_content = rv.text  # 最後手段
                        
                        if is_debug:
                            has_t03 = 'T03' in html_content
                            has_garbled = 'æŽ¥è§¸æ–¹å¼' in html_content or 'Ã¦Å½' in html_content
                            crawler_logger.info(f"[DEBUG] WorkID={work_id} HTML長度={len(html_content)}, 有T03={has_t03}, 有亂碼={has_garbled}")
                            # 儲存 HTML 以便檢查
                            #Path(f"debug_visit_html/view_{work_id}_{v['date']}.html").write_text(html_content, encoding="utf-8")
                        
                        ans, ts = parse_contact_from_view(html_content, work_id=work_id, debug=is_debug)
                        contact_answer = ans
                        contact_time = ts
                        
                        if is_debug:
                            crawler_logger.info(f"[DEBUG] WorkID={work_id} 解析結果: ans='{ans}', ts='{ts}'")
                        
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
            
            # 新增：抓取訪視問卷 T16
            try:
                visit_survey_url = get_visit_survey_url(session, work_id, project, wave)
                if visit_survey_url:
                    visit_url_abs = urljoin(BASE_URL, visit_survey_url)
                    rv_visit = session.get(visit_url_abs, timeout=TIMEOUT, allow_redirects=True)
                    if rv_visit.status_code == 200:
                        visit_html = rv_visit.content.decode('utf-8', errors='replace')
                        t16_answer = parse_t16_from_visit_survey(visit_html, work_id=work_id, debug=is_debug)
                        
                        if is_debug:
                            crawler_logger.info(f"[DEBUG] WorkID={work_id} T16答案: {t16_answer}")
            except Exception as e:
                crawler_logger.debug(f"獲取 T16 失敗 WorkID={work_id}: {e}")
            
            # 新增：檢查三個問卷狀態（每個 work 只需檢查一次，所以放在外層）
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
        
        # 在處理完所有訪次後，統一檢查問卷狀態並更新所有訪次列
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
    """主爬取邏輯（並行處理），加入進度回呼函式"""
    
    #DEBUG_DIR = output_dir / "debug_visit_html"
    #DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    
    update_progress_callback(0, 100, "1/4: 嘗試登入並獲取清單...")
    
    first_url = urljoin(BASE_URL, LIST_PATH_TMPL.format(project=project, wave=wave, page=1))
    r0 = session.get(first_url, timeout=TIMEOUT, allow_redirects=True)
    r0.raise_for_status()
    #(output_dir / "list_page_1.html").write_text(r0.text, encoding="utf-8")
    
    items, max_page = parse_list_page_for_items(r0.text)
    crawler_logger.info(f"偵測到 {max_page} 個分頁")
    
    if max_page > 1:
        update_progress_callback(10, 100, "1/4: 抓取所有清單頁面...")
        for p in range(2, max_page + 1):
            url = urljoin(BASE_URL, LIST_PATH_TMPL.format(project=project, wave=wave, page=p))
            r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
            #if r.status_code != 200:
            #    crawler_logger.warning(f"Page {p}: status {r.status_code}")
            #    continue
            #(output_dir / f"list_page_{p}.html").write_text(r.text, encoding="utf-8")
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
            # 傳入進度更新回呼函式
            future = executor.submit(
                process_single_item_v2, 
                worker_session, item, project, wave, idx, len(items), debug_work_ids, 
                # 為了避免在主執行緒更新 GUI 導致錯誤，這裡只傳遞一個將訊息放入佇列的函式
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
                
                # 更新總進度條
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
    # 邏輯與 v6.0.1 相同
    if not rows:
        crawler_logger.info("無資料可寫出")
        return ""
    
    # 定義固定的欄位順序
    fieldnames = [
        "SampleID", "WorkID", "Date", "Session", "ResultCode", "RecordURL",
        "ViewURL", "LogsURL", "InterviewerNo", "InterviewerName",
        "ContactMethod", "ContactAnsweredAt", "T16Answer",
        "Sampling", "SamplingQ", "InterviewRecord", "HasFill",
    ]
    
    # 確保所有列都有這些欄位（缺少的補空字串）
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
# 核心功能區塊 - 來自 v1.1_check.py (檢查)
# 邏輯保持不變
# =================================================================

# ------------------------------ 小工具 ------------------------------

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
    fmts = [
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
    ]
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


# ------------------------------ 規則實作 ------------------------------

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

    # 標記每個 SampleID 是否最終有代碼 100
    sample_has_100 = g.groupby("SampleID")["ResultCode3"].apply(lambda x: "100" in x.values).to_dict()
    g["SampleHas100"] = g["SampleID"].map(sample_has_100)

    # 標記是否之後有允許的較新代碼
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

        # A. 該列無結果代碼，不得有訪視問卷/戶抽/戶抽問卷/訪問記錄問卷
        if not has_rc:
            if row["T16Filled"] or row["SamplingFilled"] or row["SamplingQFilled"] or row["InterviewRecordFilled"]:
                push(row, "【問卷填寫】無結果代碼卻出現訪視問卷/戶抽/戶抽問卷/訪問記錄問卷", "II.問卷填寫")
            continue

        # B. 只要有結果代碼就要有訪視問卷（非未填）
        if not row["T16Filled"]:
            push(row, "【問卷填寫】訪視問卷未填", "II.問卷填寫")

        # C. 禁填代碼不得有戶抽/戶抽問卷/訪問記錄問卷，除非之後有 allowed_newer 代碼
        if code3 in forbidden:
            if row["SamplingFilled"] or row["SamplingQFilled"] or row["InterviewRecordFilled"]:
                if not row["HasFutureAllowed"] and not row["SampleHas100"]:
                    push(row, "【問卷填寫】此結果代碼不應有填戶抽/戶抽問卷/訪問記錄問卷，請重新檢查", "II.問卷填寫")
                    
        # D. must_have_sampling 代碼必須有戶抽與戶抽問卷
        if code3 in must_have_sampling:
            if not (row["SamplingFilled"] and row["SamplingQFilled"]):
                push(row, "【問卷填寫】此代碼需填戶抽與戶抽問卷", "II.問卷填寫")

    # E. 該樣本編號最新訪次若結果代碼=100，四欄不得為未填
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

        # A. Contact=警衛 -> 訪視問卷必須包含 '3' 或 文字包含"警衛"
        if contact_is_guard(contact):
            if ("3" not in t16_nums) and ("警衛" not in t16):
                push(row, "【問卷內容】接觸方式為警衛，但訪視問卷未包含『警衛或管理員』")

        # B. Contact=對講機 -> 訪視問卷必須包含 '2' 或 文字包含"對講機"
        if "對講機" in contact:
            if ("2" not in t16_nums) and ("對講機" not in t16):
                push(row, "【問卷內容】接觸方式為對講機，但訪視問卷未包含『對講機』")

        # C. 結果代碼=304 -> Contact 必為警衛
        if code3 == "304":
            if not contact_is_guard(contact):
                push(row, "【問卷內容】結果代碼為304，但接觸方式並非『警衛』")

        # D. 結果代碼 ∈ {311,312} -> Contact 必為公職人員
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
    """執行所有檢查並寫出結果"""
    
    update_progress_callback(96, 100, "4/4: 讀取資料並準備檢查...")
    try:
        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", na_filter=False)
    except Exception as e:
        messagebox.showerror("錯誤", f"讀取爬蟲結果 CSV 失敗：{e}")
        return False, 0
    
    df.columns = [c.strip() for c in df.columns]

    # 衍生欄位
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

    # 執行所有檢查
    update_progress_callback(97, 100, "4/4: 執行邏輯一致性檢查...")
    all_issues = []
    all_issues.extend(check_I_three_visits(df))
    all_issues.extend(check_II_questionnaire(df))
    all_issues.extend(check_III_content(df))
    all_issues.extend(check_IV_latest_codes(df))

    # 按訪員分組
    issues_df = pd.DataFrame(all_issues)
    
    if len(issues_df) == 0:
        crawler_logger.info("恭喜！沒有發現任何問題。")
        # 仍輸出空的彙總檔
        summary = pd.DataFrame([{"訪員姓名": "全部", "違規總數": 0}])
        summary_path = output_dir / "check_summary_by_interviewer.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        return True, 0

    update_progress_callback(98, 100, "4/4: 輸出違規清單檔案...")
    # 按訪員輸出個別檔案
    for interviewer, grp in issues_df.groupby("訪員姓名"):
        safe_name = re.sub(r'[\\/:*?"<>|]', '_', str(interviewer))
        filename = f"interviewer_{safe_name}.csv"
        grp_sorted = grp.sort_values(["樣本編號", "日期"])
        grp_sorted[["樣本編號", "日期", "結果代碼", "問題描述", "檢查類別"]].to_csv(
            output_dir / filename, index=False, encoding="utf-8-sig"
        )
        crawler_logger.info(f"已輸出：{filename} ({len(grp)} 筆問題)")

    # 彙總統計
    summary = issues_df.groupby("訪員姓名").size().reset_index(name="違規總數")
    summary = summary.sort_values("違規總數", ascending=False)
    summary_path = output_dir / "check_summary_by_interviewer.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    
    total_issues = len(issues_df)
    crawler_logger.info(f"\n完成！共發現 {total_issues} 個問題，涉及 {len(summary)} 位訪員")
    return True, total_issues


# =================================================================
# GUI 區塊 (使用 Tkinter)
# =================================================================

class VisitCrawlerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("訪次檢查工具 By.莊旻叡")
        self.geometry("600x450")
        
        # 狀態變數
        self.email_var = tk.StringVar(value="")
        self.password_var = tk.StringVar()
        self.project_var = tk.StringVar(value="35") # 預設值
        self.wave_var = tk.StringVar(value="99") # 預設值
        self.holiday_path_var = tk.StringVar(value="選擇國定假日清單 (選填)")
        self.output_dir = Path.cwd() / "Output" # 預設輸出到當前目錄的 Output 資料夾
        
        # 建立 GUI 元件
        self._create_widgets()
        self.bind('<Return>', lambda e: self._start_crawl_thread())
        
    def _create_widgets(self):
        # 輸入框架
        input_frame = ttk.LabelFrame(self, text="登入與專案設定", padding="10 10 10 10")
        input_frame.pack(padx=10, pady=10, fill="x")

        # 網格配置
        input_frame.columnconfigure(0, weight=1)
        input_frame.columnconfigure(1, weight=3)

        # 帳號
        ttk.Label(input_frame, text="帳號 (Email):").grid(row=0, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.email_var).grid(row=0, column=1, sticky="ew", pady=5, padx=5)

        # 密碼
        ttk.Label(input_frame, text="密碼:").grid(row=1, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.password_var, show="*").grid(row=1, column=1, sticky="ew", pady=5, padx=5)

        # Project ID
        ttk.Label(input_frame, text="Project ID:").grid(row=2, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.project_var).grid(row=2, column=1, sticky="ew", pady=5, padx=5)

        # Wave ID
        ttk.Label(input_frame, text="Wave ID:").grid(row=3, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.wave_var).grid(row=3, column=1, sticky="ew", pady=5, padx=5)

        # 假日清單按鈕
        ttk.Button(input_frame, text="選擇假日清單檔案", command=self._select_holiday_file).grid(row=4, column=0, sticky="ew", pady=5, padx=5)
        self.holiday_label = ttk.Label(input_frame, textvariable=self.holiday_path_var, wraplength=400)
        self.holiday_label.grid(row=4, column=1, sticky="w", pady=5, padx=5)

        # 執行按鈕
        self.run_button = ttk.Button(self, text="開始爬取與檢查", command=self._start_crawl_thread)
        self.run_button.pack(pady=10)

        # 進度條框架
        progress_frame = ttk.LabelFrame(self, text="執行進度", padding="10 10 10 10")
        progress_frame.pack(padx=10, pady=10, fill="x")
        
        # 進度條
        self.progress = ttk.Progressbar(progress_frame, orient="horizontal", length=500, mode="determinate")
        self.progress.pack(pady=5, fill="x")
        
        # 狀態標籤
        self.status_label = ttk.Label(progress_frame, text="待命...")
        self.status_label.pack(pady=5, fill="x")

        # 輸出路徑
        ttk.Label(self, text=f"輸出資料夾: {self.output_dir}").pack(pady=(0, 5))
        
    def _select_holiday_file(self):
        """選擇國定假日清單檔案"""
        file_path = filedialog.askopenfilename(
            title="選擇國定假日清單 (.txt)",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if file_path:
            self.holiday_path_var.set(file_path)
        else:
            self.holiday_path_var.set("選擇國定假日清單 (選填)")

    def _update_progress(self, current, total, message):
        """主執行緒安全地更新進度條和狀態"""
        percentage = (current / total) * 100
        self.progress["value"] = percentage
        self.status_label["text"] = f"{message} ({percentage:.1f}%)"
        self.update_idletasks()
        
    def _start_crawl_thread(self):
        """啟動一個獨立執行緒來執行爬蟲邏輯"""
        
        # 驗證輸入
        email = self.email_var.get().strip()
        password = self.password_var.get().strip()
        project_id = self.project_var.get().strip()
        wave_id = self.wave_var.get().strip()
        holiday_path = self.holiday_path_var.get().strip()
        
        if not email or "@" not in email:
            messagebox.showerror("錯誤", "請輸入有效的 Email 帳號。")
            return
        if not password:
            messagebox.showerror("錯誤", "請輸入密碼。")
            return
        if not project_id.isdigit() or not wave_id.isdigit():
            messagebox.showerror("錯誤", "Project ID 和 Wave ID 必須是數字。")
            return
            
        self.run_button["state"] = "disabled"
        self.progress["value"] = 0
        self.status_label["text"] = "初始化..."
        
        # 確保輸出目錄存在
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            output_csv = str(self.output_dir / "visit_records.csv")
        except Exception as e:
            messagebox.showerror("錯誤", f"無法建立輸出目錄: {e}")
            self.run_button["state"] = "normal"
            return
            
        # 處理假日清單路徑
        if holiday_path == "選擇國定假日清單 (選填)":
            holiday_path = ""
        
        # 啟動執行緒
        threading.Thread(
            target=self._run_crawl_and_check, 
            args=(email, password, int(project_id), int(wave_id), output_csv, holiday_path),
            daemon=True
        ).start()

    def _run_crawl_and_check(self, email, password, project, wave, output_csv, holiday_path):
        """在執行緒中運行核心爬蟲和檢查邏輯"""
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
                    f"資料匯出與檢查成功！\n\n檔案已輸出至：{self.output_dir}\n\n共發現 {total_issues} 個問題。\n\n本程式由莊旻叡撰寫\n特別感謝陳逸龍教授博士加先生的協助開發"
                )
            else:
                messagebox.showwarning("警告", f"資料爬取與檢查成功，但檢查過程中發生錯誤或未生成彙總檔案。\n輸出路徑：{self.output_dir}")

        except requests.exceptions.HTTPError as e:
            messagebox.showerror("錯誤", f"HTTP 錯誤: 檢查您的 Project/Wave ID 或登入狀態。\n錯誤細節: {e}")
            self._update_progress(0, 100, "錯誤：HTTP 失敗。")
        except RuntimeError as e:
            messagebox.showerror("錯誤", f"執行錯誤: {e}")
            self._update_progress(0, 100, "錯誤：登入或執行失敗。")
        except Exception as e:
            messagebox.showerror("嚴重錯誤", f"發生無法預期的錯誤: {e}")
            self._update_progress(0, 100, "錯誤：執行失敗。")
        finally:
            self.run_button["state"] = "normal"


if __name__ == "__main__":
    # 確保 logger 的輸出不會被 GUI 介面吞掉
    # 創建一個檔案 handler 以便在打包後仍能記錄日誌
    try:
        log_dir = Path.cwd() / "logs"
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_dir / 'crawler_log.txt', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        crawler_logger.addHandler(file_handler)
    except Exception as e:
        # 如果無法建立日誌檔，仍讓程式繼續執行
        print(f"無法設定日誌檔案: {e}")
        
    app = VisitCrawlerApp()
    app.mainloop()