"""
JM.ASSET AI v3.0 — DART + 네이버증권 스크래퍼
================================================
PC 없이 GitHub Actions에서 매일 자동 실행
- DART Open API  : 재무데이터 (유보율·부채비율·이자보상배율)
- 네이버증권     : 주가·일봉·거래량·시가총액
- 카카오톡       : 조건 충족 종목 알림 발송
"""

import os, time, json, requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════
#  설정값 (GitHub Secrets에서 주입)
# ═══════════════════════════════════════════════
DART_API_KEY    = os.environ.get("DART_API_KEY", "")       # DART Open API 키
KAKAO_TOKEN     = os.environ.get("KAKAO_TOKEN", "")        # 카카오 액세스 토큰
OUTPUT_FILE     = "data/results.json"                       # 결과 저장 파일

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36"
}

# ═══════════════════════════════════════════════
#  전략 조건
# ═══════════════════════════════════════════════
STRATEGY = {
    "reserve_ratio_min":  300,    # 유보율 300% 이상
    "debt_ratio_max":     200,    # 부채비율 200% 이하
    "icr_min":            1.0,    # 이자보상배율 1.0배 이상
    "mktcap_min":         1000,   # 시가총액 1000억 이상 (억원)
    "avg_amount_min":     3,      # 10일 평균 거래대금 3억 이상 (억원)
    "vol_increase_min":   200,    # 거래량 전일대비 200% 이상
    "bb_period":          40,     # 볼린저밴드 기간
    "bb_mult":            2.0,    # 볼린저밴드 승수
    "bb_upper_min":      -7.0,    # BB 상한선 대비 최소 -7%
    "bb_upper_max":       3.0,    # BB 상한선 대비 최대 +3%
    "ma_short":           112,    # 단기 이평
    "ma_mid":             224,    # 중기 이평
    "ma_long":            448,    # 장기 이평
}


# ═══════════════════════════════════════════════
#  1. DART — 전 종목 재무데이터 수집
# ═══════════════════════════════════════════════
def get_dart_financials():
    """
    DART Open API에서 전 종목 재무비율 한번에 가져오기
    (유보율·부채비율·이자보상배율)
    반환: {종목코드: {reserve, debt, icr, name}}
    """
    print("[DART] 재무데이터 수집 시작...")

    if not DART_API_KEY:
        print("[DART] API 키 없음 → 데모 데이터 사용")
        return _dart_demo()

    year  = str(datetime.now().year - 1)   # 전년도 결산 기준
    reprt = "11011"                         # 사업보고서

    # 재무비율 API (fnlttSinglAcntAll)
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "bsns_year":  year,
        "reprt_code": reprt,
        "fs_div":     "CFS",   # 연결재무제표 우선
    }

    result = {}
    try:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()

        if data.get("status") != "000":
            print(f"[DART] 오류: {data.get('message')}")
            return _dart_demo()

        for item in data.get("list", []):
            code    = item.get("stock_code", "").strip()
            acnt_nm = item.get("account_nm", "")
            val_str = item.get("thstrm_amount", "0").replace(",","").replace("-","0")

            if not code or len(code) != 6:
                continue

            if code not in result:
                result[code] = {"name": item.get("corp_name",""), "reserve":0, "debt":999, "icr":0}

            try:
                val = float(val_str) if val_str else 0
            except:
                val = 0

            if "유보율" in acnt_nm:
                result[code]["reserve"] = val
            elif "부채비율" in acnt_nm:
                result[code]["debt"] = val
            elif "이자보상배율" in acnt_nm:
                result[code]["icr"] = val

        print(f"[DART] {len(result)}개 종목 재무데이터 수집 완료")
    except Exception as e:
        print(f"[DART] 수집 오류: {e} → 데모 데이터 사용")
        return _dart_demo()

    return result


def _dart_demo():
    """데모용 재무 데이터"""
    return {
        "195870": {"name":"해성디에스",    "reserve":980,  "debt":42,  "icr":8.2},
        "214150": {"name":"클래시스",      "reserve":1200, "debt":18,  "icr":12.4},
        "007660": {"name":"이수페타시스",  "reserve":670,  "debt":88,  "icr":4.1},
        "001720": {"name":"신영증권",      "reserve":2100, "debt":55,  "icr":6.8},
        "058470": {"name":"리노공업",      "reserve":1450, "debt":12,  "icr":22.0},
        "213420": {"name":"덕산네오룩스",  "reserve":560,  "debt":25,  "icr":5.3},
        "056190": {"name":"에스에프에이",  "reserve":820,  "debt":48,  "icr":9.7},
        "036930": {"name":"주성엔지니어링","reserve":2050, "debt":28,  "icr":18.5},
        "080220": {"name":"제주반도체",    "reserve":520,  "debt":30,  "icr":5.0},
        "033100": {"name":"제룡전기",      "reserve":980,  "debt":22,  "icr":11.2},
        "358600": {"name":"지아이이노베이션","reserve":510, "debt":40,  "icr":3.8},
        "012510": {"name":"더존비즈온",    "reserve":750,  "debt":62,  "icr":7.2},
    }


# ═══════════════════════════════════════════════
#  2. 네이버증권 — 종목 리스트 (전체 상장사)
# ═══════════════════════════════════════════════
def get_all_stock_codes():
    """네이버증권에서 코스피+코스닥 전 종목 코드 수집"""
    print("[네이버] 전 종목 코드 수집 중...")
    codes = []

    for market in ["KOSPI", "KOSDAQ"]:
        page = 1
        while True:
            url = (f"https://finance.naver.com/sise/sise_market_sum.nhn"
                   f"?sosok={'0' if market=='KOSPI' else '1'}&page={page}")
            try:
                r = requests.get(url, headers=HEADERS, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                links = soup.select("td.name a")
                if not links:
                    break
                for a in links:
                    href = a.get("href","")
                    if "code=" in href:
                        code = href.split("code=")[-1].strip()
                        if len(code) == 6 and code.isdigit():
                            codes.append(code)
                page += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"[네이버] 페이지 오류 p{page}: {e}")
                break

    codes = list(set(codes))
    print(f"[네이버] 총 {len(codes)}개 종목 코드 수집")
    return codes


# ═══════════════════════════════════════════════
#  3. 네이버증권 — 종목 일봉 데이터 (500일)
# ═══════════════════════════════════════════════
def get_daily_prices(code, count=500):
    """
    네이버증권 일봉 API로 종가·거래량·시가총액 수집
    반환: [{"date","close","volume","mktcap"}, ...]  최신순
    """
    url = (f"https://fchart.stock.naver.com/sise.nhn"
           f"?symbol={code}&timeframe=day&count={count}&requestType=0")
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "xml")
        items = soup.find_all("item")
        candles = []
        for item in items:
            d = item.get("data","").split("|")
            if len(d) >= 5:
                try:
                    candles.append({
                        "date":   d[0],
                        "close":  abs(int(d[4])),   # 종가
                        "volume": int(d[5]) if len(d) > 5 else 0,
                        "open":   abs(int(d[1])),
                        "high":   abs(int(d[2])),
                        "low":    abs(int(d[3])),
                    })
                except:
                    pass
        candles.reverse()   # 오래된 순 → 최신순으로
        candles.reverse()   # 최신이 앞으로
        return candles
    except Exception as e:
        return []


def get_stock_info(code):
    """네이버증권 종목 기본정보 (시가총액·종목명)"""
    url = f"https://finance.naver.com/item/main.nhn?code={code}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        name_el = soup.select_one("div.wrap_company h2 a")
        name = name_el.text.strip() if name_el else code

        # 시가총액 (억원)
        mktcap = 0
        for tr in soup.select("table.tb_type1 tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td and "시가총액" in th.text:
                val = td.text.strip().replace(",","").replace("억원","").strip()
                try:
                    mktcap = int(val)
                except:
                    pass
                break

        return {"name": name, "mktcap": mktcap}
    except:
        return {"name": code, "mktcap": 0}


# ═══════════════════════════════════════════════
#  4. 이동평균 / 볼린저 계산
# ═══════════════════════════════════════════════
def sma(prices, n):
    if len(prices) < n:
        return None
    return sum(prices[-n:]) / n

def sma_list(prices, n):
    if len(prices) < n:
        return []
    return [sum(prices[i:i+n])/n for i in range(len(prices)-n+1)]

def ema_last(prices, n):
    if len(prices) < n:
        return None
    k = 2 / (n + 1)
    v = sum(prices[:n]) / n
    for p in prices[n:]:
        v = p * k + v * (1 - k)
    return v

def bollinger_upper(prices, n=40, mult=2.0):
    if len(prices) < n:
        return None
    e = ema_last(prices, n)
    if e is None:
        return None
    std = (sum((p - e) ** 2 for p in prices[-n:]) / n) ** 0.5
    return e + mult * std

def avg_amount_10d(candles):
    total, cnt = 0, 0
    for c in candles[:10]:
        try:
            total += c["close"] * c["volume"]
            cnt += 1
        except:
            pass
    return round(total / cnt / 1e8, 1) if cnt else 0   # 억원

def vol_increase(candles):
    try:
        return round(candles[0]["volume"] / candles[1]["volume"] * 100, 1)
    except:
        return 0


# ═══════════════════════════════════════════════
#  5. 전략 필터 — 종목 하나 분석
# ═══════════════════════════════════════════════
def analyze(code, fin, candles, info):
    S = STRATEGY

    # ── 재무 필터 ──
    if fin["reserve"] < S["reserve_ratio_min"]:  return None
    if fin["debt"]    > S["debt_ratio_max"]:      return None
    if fin["icr"]     < S["icr_min"]:             return None

    # ── 시가총액 ──
    mktcap = info.get("mktcap", 0)
    if mktcap < S["mktcap_min"]:
        return None

    # ── 일봉 데이터 충분한지 ──
    if len(candles) < S["ma_long"] + 10:
        return None

    closes = [c["close"] for c in candles]
    price  = closes[0]   # 최신 종가

    # ── 이동평균 ──
    ma112 = sma(closes, S["ma_short"])
    ma224 = sma(closes, S["ma_mid"])
    ma448 = sma(closes, S["ma_long"])
    if not (ma112 and ma224 and ma448):
        return None

    # ── 역배열 확인 ──
    if not (ma112 < ma224 < ma448):
        return None

    # ── 구간 판단 ──
    if   ma112 < price < ma224:  group = "A"
    elif ma224 < price < ma448:  group = "B"
    else:                         return None

    # ── 거래대금 (10일 평균 3억↑) ──
    avg_amt = avg_amount_10d(candles)
    if avg_amt < S["avg_amount_min"]:
        return None

    # ── 거래량 급증 (전일대비 200%↑) ──
    vol_inc = vol_increase(candles)
    if vol_inc < S["vol_increase_min"]:
        return None

    # ── 볼린저밴드 상한선 (-7% ~ +3%) ──
    upper = bollinger_upper(closes, S["bb_period"], S["bb_mult"])
    if not upper:
        return None
    bb_dist = round((price - upper) / upper * 100, 2)
    if not (S["bb_upper_min"] <= bb_dist <= S["bb_upper_max"]):
        return None

    return {
        "code":         code,
        "name":         fin.get("name") or info.get("name", code),
        "group":        group,
        "price":        price,
        "mktcap":       mktcap,
        "reserveRatio": round(fin["reserve"], 1),
        "debtRatio":    round(fin["debt"], 1),
        "icr":          round(fin["icr"], 1),
        "ma112":        round(ma112),
        "ma224":        round(ma224),
        "ma448":        round(ma448),
        "bbUpper":      round(upper),
        "bbDist":       bb_dist,
        "volInc":       round(vol_inc),
        "avgAmt10d":    avg_amt,
        "scannedAt":    datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ═══════════════════════════════════════════════
#  6. 카카오톡 알림 발송
# ═══════════════════════════════════════════════
def send_kakao(results):
    if not KAKAO_TOKEN:
        print("[카카오] 토큰 없음 → 알림 생략")
        return

    a_list = [r for r in results if r["group"] == "A"]
    b_list = [r for r in results if r["group"] == "B"]

    now   = datetime.now().strftime("%Y년 %m월 %d일 %H:%M")
    total = len(results)

    if total == 0:
        msg = f"📊 JM.ASSET AI\n{now}\n\n오늘은 조건 충족 종목이 없습니다."
    else:
        lines = [f"📊 JM.ASSET AI 스캔 완료", f"📅 {now}", f"총 {total}종목 발견\n"]

        if a_list:
            lines.append(f"🟢 A그룹 (안정형) {len(a_list)}종목")
            for r in a_list[:5]:
                lines.append(f"  · {r['name']} ({r['code']})")
                lines.append(f"    {r['price']:,}원 | BB {r['bbDist']:+.1f}% | 거래량+{r['volInc']}%")

        if b_list:
            lines.append(f"\n🔵 B그룹 (공격형) {len(b_list)}종목")
            for r in b_list[:5]:
                lines.append(f"  · {r['name']} ({r['code']})")
                lines.append(f"    {r['price']:,}원 | BB {r['bbDist']:+.1f}% | 거래량+{r['volInc']}%")

        if total > 10:
            lines.append(f"\n... 외 {total-10}종목 (앱에서 전체 확인)")

        msg = "\n".join(lines)

    try:
        r = requests.post(
            "https://kapi.kakao.com/v2/api/talk/memo/default/send",
            headers={"Authorization": f"Bearer {KAKAO_TOKEN}"},
            data={
                "template_object": json.dumps({
                    "object_type": "text",
                    "text": msg,
                    "link": {"web_url": "", "mobile_web_url": ""}
                })
            },
            timeout=10
        )
        result = r.json()
        if result.get("result_code") == 0:
            print(f"[카카오] 알림 발송 성공 ✅ ({total}종목)")
        else:
            print(f"[카카오] 발송 실패: {result}")
    except Exception as e:
        print(f"[카카오] 오류: {e}")


# ═══════════════════════════════════════════════
#  7. 메인 실행
# ═══════════════════════════════════════════════
def main():
    print("=" * 55)
    print("  JM.ASSET AI v3.0 — 자동 스캔 시작")
    print(f"  실행시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # 1. DART 재무 데이터
    financials = get_dart_financials()
    print(f"[재무] {len(financials)}개 종목 로드")

    # 2. 재무 필터 통과한 종목만 추려서 주가 조회 (시간 절약)
    pre_filtered = {
        code: fin for code, fin in financials.items()
        if (fin["reserve"] >= STRATEGY["reserve_ratio_min"] and
            fin["debt"]    <= STRATEGY["debt_ratio_max"]    and
            fin["icr"]     >= STRATEGY["icr_min"])
    }
    print(f"[재무필터] {len(pre_filtered)}개 통과 → 주가 조회 시작")

    results = []
    for i, (code, fin) in enumerate(pre_filtered.items()):
        print(f"  [{i+1}/{len(pre_filtered)}] {fin.get('name',code)} ({code}) 분석 중...", end=" ")

        # 주가 일봉
        candles = get_daily_prices(code, count=500)
        if not candles:
            print("❌ 일봉 없음")
            continue

        # 시가총액
        info = get_stock_info(code)

        # 전략 분석
        result = analyze(code, fin, candles, info)
        if result:
            results.append(result)
            print(f"✅ {result['group']}그룹!")
        else:
            print("—")

        time.sleep(0.3)   # 네이버 요청 제한 방지

    print(f"\n[완료] {len(results)}종목 조건 충족")
    for r in results:
        print(f"  {r['group']}그룹 | {r['name']}({r['code']}) | {r['price']:,}원 | BB{r['bbDist']:+.1f}%")

    # 3. 결과 저장
    os.makedirs("data", exist_ok=True)
    output = {
        "scannedAt": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total":     len(results),
        "stocks":    results,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[저장] {OUTPUT_FILE} 저장 완료")

    # 4. 카카오톡 알림
    send_kakao(results)

    print("=" * 55)
    print("  스캔 완료!")
    print("=" * 55)


if __name__ == "__main__":
    main()
