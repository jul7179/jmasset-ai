"""
ChargeMap — 공공데이터 자동 수집기
=====================================
GitHub Actions에서 매일 실행
- API ①: 한국환경공단 전기차 충전소 위치·운영정보
  Base: api.odcloud.kr/api/15119741/v1/uddi:fe904caf-636f-4a49-aa94-e9064a446b3e
- API ②: 환경부 공공급속 충전기 충전정보 (월별 단가)
  Base: apis.data.go.kr/B552584/pbnstFstChrgrChgcpcyInfo
결과: data/ev_stations.json, data/ev_prices.json
"""

import os, json, time, requests
from datetime import datetime, timedelta
from pathlib import Path

# ── 설정 ──
API_KEY   = os.environ.get('DART_API_KEY') or os.environ.get('EV_API_KEY', '')
API1_BASE = 'https://api.odcloud.kr/api/15119741/v1/uddi:fe904caf-636f-4a49-aa94-e9064a446b3e'
API2_BASE = 'https://apis.data.go.kr/B552584/pbnstFstChrgrChgcpcyInfo/getYrMnChgcpcyInfo'
OUT_DIR   = Path('data')

HEADERS = {'User-Agent': 'ChargeMap/4.0 (github-actions)'}

# 시설구분 분류
FAC_KEYWORDS = {
    'mart':  ['마트','대형마트','슈퍼마켓','이마트','롯데마트','홈플러스','코스트코','하이마트','트레이더스'],
    'mall':  ['쇼핑몰','백화점','아울렛','면세점','복합쇼핑','쇼핑센터','코엑스','타임스퀘어','스타필드'],
    'park':  ['주차장','주차','공영주차','민영주차','노외주차'],
    'pub':   ['공공','관공서','주민센터','구청','시청','군청','경찰','소방','학교','대학','병원','보건소','복지관','공단','공사'],
    'road':  ['휴게소','주유소','고속도로','도로공사','터미널','공항','역사'],
}

def classify_facility(row: dict) -> str:
    text = ' '.join([
        str(row.get('시설구분(대)', '')),
        str(row.get('시설구분(소)', '')),
        str(row.get('충전소명', '')),
        str(row.get('주소', '')),
    ]).lower()
    for ftype, keys in FAC_KEYWORDS.items():
        if any(k.lower() in text for k in keys):
            return ftype
    return 'etc'

# ═══════════════════════════════════════
#  API ① — 충전소 위치·상태 수집
# ═══════════════════════════════════════
def fetch_stations() -> list:
    print(f'[API①] 충전소 위치·상태 수집 시작...')
    all_items = []
    page = 1
    per_page = 1000

    while True:
        params = {
            'serviceKey': API_KEY,
            'page':       page,
            'perPage':    per_page,
        }
        try:
            r = requests.get(API1_BASE, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            items = data.get('data', [])
            total = data.get('totalCount', 0)

            if not items:
                break

            all_items.extend(items)
            print(f'  페이지 {page}: {len(items)}건 (누적 {len(all_items)}/{total})')

            if len(all_items) >= total or len(items) < per_page:
                break

            page += 1
            time.sleep(0.5)

        except Exception as e:
            print(f'  [오류] 페이지 {page}: {e}')
            break

    print(f'[API①] 총 {len(all_items)}건 수집')
    return all_items

def normalize_station(row: dict) -> dict:
    """API① 데이터 → 앱 표준 형식"""
    return {
        'code':     str(row.get('충전소ID', row.get('statId', ''))),
        'name':     row.get('충전소명', row.get('statNm', '')).strip(),
        'addr':     row.get('주소', row.get('addr', '')).strip(),
        'addrDetail': row.get('상세주소', '').strip(),
        'lat':      float(row.get('위도', row.get('lat', 0)) or 0),
        'lng':      float(row.get('경도', row.get('lng', 0)) or 0),
        'stat':     str(row.get('충전기상태', row.get('stat', '9'))),
        'chargerType': row.get('충전기타입', row.get('chgerType', '')),
        'output':   str(row.get('충전용량', row.get('output', ''))),
        'useTime':  row.get('이용가능시간', row.get('useTime', '24시간')),
        'busiNm':   row.get('운영기관', row.get('busiNm', '')),
        'busiCall': row.get('운영기관연락처', row.get('busiCall', '')),
        'parkingFree': row.get('주차료무료여부', row.get('parkingFree', 'N')) == 'Y',
        'limitYn':  row.get('이용자제한여부', row.get('limitYn', 'N')),
        'facilityLarge': row.get('시설구분(대)', ''),
        'facilitySmall': row.get('시설구분(소)', ''),
        'facType':  classify_facility(row),
        'floor':    row.get('지상/지하', ''),
        'lastStatUpdateDt': row.get('상태갱신일시', ''),
        'lastChargeStartDt': row.get('마지막충전시작일시', ''),
    }

# ═══════════════════════════════════════
#  API ② — 월별 단가 수집
# ═══════════════════════════════════════
def fetch_prices() -> list:
    now  = datetime.now()
    # 전월 기준 (당월은 미확정)
    prev = now.replace(day=1) - timedelta(days=1)
    ym   = prev.strftime('%Y%m')
    print(f'[API②] 월별 단가 수집 ({ym}) 시작...')

    all_items = []
    page = 1
    per_page = 100

    while True:
        params = {
            'serviceKey': API_KEY,
            'pageNo':     page,
            'numOfRows':  per_page,
            'dataType':   'JSON',
            'yrMn':       ym,
        }
        try:
            r = requests.get(API2_BASE, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            text = r.text

            # JSON 파싱
            try:
                data  = r.json()
                body  = data.get('response', {}).get('body', {})
                items = body.get('items', {}).get('item', [])
                total = int(body.get('totalCount', 0))
                if not isinstance(items, list):
                    items = [items] if items else []
            except Exception:
                # XML 파싱
                import xml.etree.ElementTree as ET
                root  = ET.fromstring(text)
                items = []
                for item in root.findall('.//item'):
                    d = {child.tag: child.text for child in item}
                    items.append(d)
                total = len(items)

            if not items:
                break

            all_items.extend(items)
            print(f'  페이지 {page}: {len(items)}건 (누적 {len(all_items)}/{total})')

            if len(all_items) >= total or len(items) < per_page:
                break

            page += 1
            time.sleep(0.5)

        except Exception as e:
            print(f'  [오류] 페이지 {page}: {e}')
            break

    print(f'[API②] 총 {len(all_items)}건 수집')
    return all_items

def normalize_price(row: dict, ym: str) -> dict:
    """API② 데이터 → 앱 표준 형식"""
    price = 0
    for key in ['unitPrice', 'chrgUnitPrice', 'upw']:
        try:
            v = float(row.get(key, 0) or 0)
            if v > 0:
                price = v
                break
        except:
            pass

    qty = 0
    for key in ['chrgQty', 'totalChrgQty', 'qty']:
        try:
            v = float(row.get(key, 0) or 0)
            if v > 0:
                qty = v
                break
        except:
            pass

    busi = row.get('busiNm', row.get('chrgBusiNm', ''))
    is_pub = any(k in busi for k in ['환경부','환경공단','한전','공단','공사','시청','구청'])

    return {
        'ym':       row.get('yrMn', row.get('chrgYm', ym)),
        'name':     row.get('statNm', row.get('chrgStaNm', row.get('areaNm', ''))).strip(),
        'addr':     row.get('addr', row.get('roadAddr', row.get('areaNm', ''))).strip(),
        'price':    round(price, 1),
        'qty':      round(qty, 1),
        'busiNm':   busi,
        'chargerCnt': int(row.get('chrgCnt', row.get('chgrCnt', 0)) or 0),
        'isPub':    is_pub,
    }

# ═══════════════════════════════════════
#  메인
# ═══════════════════════════════════════
def main():
    print('=' * 55)
    print('  ChargeMap 데이터 수집기 v4.0')
    print(f'  실행 시각: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 55)

    if not API_KEY:
        print('[경고] EV_API_KEY 환경변수 없음 → 데모 데이터 생성')
        save_demo()
        return

    OUT_DIR.mkdir(exist_ok=True)
    now = datetime.now()
    ym  = (now.replace(day=1) - timedelta(days=1)).strftime('%Y%m')

    # ── API① 충전소 수집 ──
    try:
        raw1    = fetch_stations()
        stations = [normalize_station(r) for r in raw1]
        # 시설구분 통계
        fac_cnt = {}
        for s in stations:
            fac_cnt[s['facType']] = fac_cnt.get(s['facType'], 0) + 1
        print(f'  시설구분 현황: {fac_cnt}')
    except Exception as e:
        print(f'[API①] 오류: {e} → 빈 배열')
        stations = []

    # ── API② 단가 수집 ──
    try:
        raw2   = fetch_prices()
        prices = [normalize_price(r, ym) for r in raw2]
        prices = [p for p in prices if p['price'] > 0]
        prices.sort(key=lambda x: x['price'])
    except Exception as e:
        print(f'[API②] 오류: {e} → 빈 배열')
        prices = []

    # ── 저장 ──
    stations_out = {
        'updatedAt': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'total':     len(stations),
        'stations':  stations,
    }
    prices_out = {
        'updatedAt': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'ym':        ym,
        'total':     len(prices),
        'prices':    prices,
    }

    with open(OUT_DIR / 'ev_stations.json', 'w', encoding='utf-8') as f:
        json.dump(stations_out, f, ensure_ascii=False, indent=2)
    print(f'[저장] data/ev_stations.json ({len(stations)}건)')

    with open(OUT_DIR / 'ev_prices.json', 'w', encoding='utf-8') as f:
        json.dump(prices_out, f, ensure_ascii=False, indent=2)
    print(f'[저장] data/ev_prices.json ({len(prices)}건)')

    print('=' * 55)
    print('  수집 완료!')
    print('=' * 55)

def save_demo():
    """API 키 없을 때 데모 데이터 생성"""
    OUT_DIR.mkdir(exist_ok=True)
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    demo_stations = [
        {'code':'S001','name':'이마트 강남점 충전소',   'addr':'서울시 강남구 테헤란로 123','addrDetail':'지하1층','lat':37.508,'lng':127.063,'stat':'2','chargerType':'DC콤보(급속)','output':'100','useTime':'24시간','busiNm':'이마트','busiCall':'1234-5678','parkingFree':True,'limitYn':'N','facilityLarge':'마트','facilitySmall':'대형마트','facType':'mart','floor':'지하1층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S002','name':'롯데마트 잠실점',        'addr':'서울시 송파구 올림픽로 300','addrDetail':'지하2층','lat':37.513,'lng':127.100,'stat':'3','chargerType':'AC완속',      'output':'7', 'useTime':'07:00~23:00','busiNm':'롯데마트','busiCall':'2345-6789','parkingFree':True,'limitYn':'N','facilityLarge':'마트','facilitySmall':'대형마트','facType':'mart','floor':'지하2층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S003','name':'코엑스몰 전기차 충전소', 'addr':'서울시 강남구 봉은사로 524','addrDetail':'지하3층','lat':37.513,'lng':127.059,'stat':'2','chargerType':'DC콤보(급속)','output':'50', 'useTime':'07:00~22:00','busiNm':'한국환경공단','busiCall':'1588-0000','parkingFree':False,'limitYn':'N','facilityLarge':'쇼핑몰','facilitySmall':'복합쇼핑몰','facType':'mall','floor':'지하3층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S004','name':'롯데백화점 본점',        'addr':'서울시 중구 남대문로 81',   'addrDetail':'지하4층','lat':37.565,'lng':126.981,'stat':'2','chargerType':'DC콤보(급속)','output':'50', 'useTime':'10:30~20:00','busiNm':'롯데백화점','busiCall':'3456-7890','parkingFree':False,'limitYn':'N','facilityLarge':'쇼핑몰','facilitySmall':'백화점','facType':'mall','floor':'지하4층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S005','name':'서울시청 공영주차장',    'addr':'서울시 중구 세종대로 110',  'addrDetail':'지하1층','lat':37.566,'lng':126.978,'stat':'2','chargerType':'AC완속',      'output':'7', 'useTime':'24시간','busiNm':'서울시설공단','busiCall':'120','parkingFree':True,'limitYn':'N','facilityLarge':'주차장','facilitySmall':'공영주차장','facType':'park','floor':'지하1층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S006','name':'강남구청 주차장',        'addr':'서울시 강남구 학동로 426',  'addrDetail':'지상1층','lat':37.517,'lng':127.047,'stat':'2','chargerType':'DC콤보(급속)','output':'50', 'useTime':'24시간','busiNm':'강남구청','busiCall':'02-3423-5555','parkingFree':True,'limitYn':'N','facilityLarge':'공공기관','facilitySmall':'관공서','facType':'pub','floor':'지상1층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S007','name':'한국환경공단 급속충전소','addr':'서울시 서초구 반포대로 20', 'addrDetail':'지상','lat':37.503,'lng':127.004,'stat':'4','chargerType':'DC콤보(급속)','output':'50', 'useTime':'24시간','busiNm':'한국환경공단','busiCall':'1577-0100','parkingFree':False,'limitYn':'N','facilityLarge':'공공기관','facilitySmall':'공공기관','facType':'pub','floor':'지상','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S008','name':'경부고속도로 안성휴게소','addr':'경기도 안성시 죽산면 죽산리','addrDetail':'지상','lat':37.116,'lng':127.227,'stat':'2','chargerType':'DC콤보(급속)','output':'100','useTime':'24시간','busiNm':'한국도로공사','busiCall':'1588-2504','parkingFree':False,'limitYn':'N','facilityLarge':'휴게소','facilitySmall':'고속도로휴게소','facType':'road','floor':'지상','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S009','name':'SK에너지 강남주유소',    'addr':'서울시 강남구 강남대로 390','addrDetail':'지상','lat':37.495,'lng':127.029,'stat':'2','chargerType':'DC콤보(급속)','output':'50', 'useTime':'24시간','busiNm':'SK에너지','busiCall':'5678-9012','parkingFree':False,'limitYn':'N','facilityLarge':'주유소','facilitySmall':'주유소','facType':'road','floor':'지상','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S010','name':'현대 E-pit 강남',        'addr':'서울시 강남구 언주로 120',  'addrDetail':'지상1층','lat':37.522,'lng':127.042,'stat':'2','chargerType':'DC콤보(급속)','output':'350','useTime':'24시간','busiNm':'현대자동차','busiCall':'080-600-6000','parkingFree':False,'limitYn':'N','facilityLarge':'기타','facilitySmall':'완성차딜러','facType':'etc','floor':'지상1층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S011','name':'홈플러스 영등포점',      'addr':'서울시 영등포구 양평로 240','addrDetail':'지하1층','lat':37.529,'lng':126.898,'stat':'2','chargerType':'DC콤보(급속)','output':'100','useTime':'08:00~23:00','busiNm':'홈플러스','busiCall':'6789-0123','parkingFree':True,'limitYn':'N','facilityLarge':'마트','facilitySmall':'대형마트','facType':'mart','floor':'지하1층','lastStatUpdateDt':'','lastChargeStartDt':''},
        {'code':'S012','name':'스타필드 하남',          'addr':'경기도 하남시 미사대로 750','addrDetail':'지하4층','lat':37.565,'lng':127.210,'stat':'2','chargerType':'DC콤보(급속)','output':'100','useTime':'10:00~22:00','busiNm':'신세계','busiCall':'7890-1234','parkingFree':False,'limitYn':'N','facilityLarge':'쇼핑몰','facilitySmall':'복합쇼핑몰','facType':'mall','floor':'지하4층','lastStatUpdateDt':'','lastChargeStartDt':''},
    ]
    demo_prices = [
        {'ym':'202503','name':'이마트 everon',    'addr':'서울시 강남구','price':280,'qty':7200,'busiNm':'이마트',    'chargerCnt':6,'isPub':False},
        {'ym':'202503','name':'롯데마트 충전소',  'addr':'서울시 송파구','price':285,'qty':8900,'busiNm':'롯데',      'chargerCnt':4,'isPub':False},
        {'ym':'202503','name':'한국도로공사 휴게소','addr':'경부고속도로','price':309,'qty':22000,'busiNm':'한국도로공사','chargerCnt':10,'isPub':True},
        {'ym':'202503','name':'한국전력공사',     'addr':'서울시 서초구','price':313,'qty':9800, 'busiNm':'한국전력공사','chargerCnt':2,'isPub':True},
        {'ym':'202503','name':'한국환경공단',     'addr':'서울시 강남구','price':324,'qty':12500,'busiNm':'한국환경공단','chargerCnt':4,'isPub':True},
        {'ym':'202503','name':'GS칼텍스',         'addr':'서울시 마포구','price':334,'qty':5600, 'busiNm':'GS칼텍스',  'chargerCnt':3,'isPub':False},
        {'ym':'202503','name':'SK에너지',         'addr':'서울시 강서구','price':340,'qty':4300, 'busiNm':'SK에너지',  'chargerCnt':2,'isPub':False},
        {'ym':'202503','name':'현대 E-pit',       'addr':'서울시 강남구','price':369,'qty':15000,'busiNm':'현대자동차','chargerCnt':8,'isPub':False},
        {'ym':'202503','name':'제주에너지공사',   'addr':'제주시 연동',  'price':150,'qty':6700, 'busiNm':'제주에너지공사','chargerCnt':4,'isPub':True},
    ]
    with open(OUT_DIR/'ev_stations.json','w',encoding='utf-8') as f:
        json.dump({'updatedAt':now_str,'total':len(demo_stations),'stations':demo_stations},f,ensure_ascii=False,indent=2)
    with open(OUT_DIR/'ev_prices.json','w',encoding='utf-8') as f:
        json.dump({'updatedAt':now_str,'ym':'202503','total':len(demo_prices),'prices':demo_prices},f,ensure_ascii=False,indent=2)
    print(f'[데모] ev_stations.json ({len(demo_stations)}건), ev_prices.json ({len(demo_prices)}건) 생성')

if __name__ == '__main__':
    main()
