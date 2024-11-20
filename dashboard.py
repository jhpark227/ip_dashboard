# 스트림릿 라이브러리를 사용하기 위한 임포트
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cx_Oracle as co
from datetime import datetime, timedelta
import altair as alt

import warnings
warnings.filterwarnings('ignore')

# 웹 대시보드 개발 라이브러리인 스트림릿은,
# main 함수가 있어야 한다.

url = 'POLSEL/"polsel01!@"@10.200.210.212:1521/MIRAEAWS'
color_map = ['#F58220', '#043B72','#00A9CE', '#F0B26B', '#8DC8E8','#CB6015','#AE634E', '#84888B','#7EA0C3', '#C2AC97', '#0086B8']

st.set_page_config(page_title='Invest Pool Dashboard', layout='wide')
today = datetime.today().strftime('%y%m%d')

def db_connect(url, sql):
	conn = co.connect(url)

	df = pd.read_sql(sql, con=conn)
	conn.close()
	return df

def calculate_period_return(group):
    start_value = group.iloc[0]["CLOSE_INDEX"]
    end_value = group.iloc[-1]["CLOSE_INDEX"]
    return np.round((end_value - start_value)/ start_value * 100,2)  # 수익률 (%)




sql_date = '''
			SELECT 
                BF_TRD_DT
                ,BF2_TRD_DT
            FROM AMAKT.FSBD_DT_INFO 
            WHERE BASE_DT = '{targetdate}'
			'''.format(targetdate=today)

date_info = db_connect(url, sql_date)
default_date = date_info['BF_TRD_DT'][0]
default_date_before = date_info['BF2_TRD_DT'][0]

def main() :


    # 사이드바(Sidebar)
    with st.sidebar:
        st.title('Invest Pool Dashboard')

        startdate = st.date_input('Start Date: ', value=default_date_before)
        enddate = st.date_input('End Date: ', value=default_date)

        type_fund = st.radio(
                            "Choose a Fund Type:",
                            ("액티브(Active)", "인덱스(Index)")
                            )


    mkt_index_sql = '''
                WITH BM_INFO AS (	
                    SELECT
                                WKDATE
                                , KFNAME
                                , CLOSE_INDEX
                            FROM AMAKT.E_MA_FN_CLOSE_INDEX
                            WHERE KFNAME IN ('코스피 200', '코스피', '코스닥')
                        )
                , RAWDATA AS (
                            SELECT 
                                A.WKDATE
                                , A.KFNAME
                                , A.CLOSE_INDEX
                            FROM BM_INFO A
                            LEFT JOIN AMAKT.FSBD_DT_INFO DT
                                ON A.WKDATE = DT.BASE_DT
                            WHERE DT.TRD_DT_YN = 'Y'
                            )

		            SELECT *
                    FROM RAWDATA 
                    WHERE WKDATE BETWEEN '{startdate}' AND '{enddate}'
    '''.format(startdate=startdate, enddate=enddate)
    
    active_sql = '''
            WITH BM_WGT AS (
				SELECT *
				FROM (
						SELECT B.JNAME
							, B.STDJONG
							, B.INDUSTRY_LEV1_NM
							, B.INDUSTRY_LEV2_NM
							, B.INDUSTRY_LEV3_NM
							, A.INDEX_NAME_KR
							, A.INDEX_WEIGHT
							, ROUND(B.RATE/100, 4) AS "일수익률"
						
						FROM AMAKT.E_MA_KRX_PKG_CONST A
						
						LEFT JOIN AMAKT.E_MA_FN_JONGMOK_INFO B
							ON A.FILE_DATE = B.WKDATE
							AND A.CONSTITUENT_ISIN = B.STDJONG
							AND B.INDEX_ID IN ('I.001', 'I.201')
						WHERE A.FILE_DATE = '{enddate}'
						AND A.INDEX_CODE1 IN ('1', '2')  -- 1:코스피, 2:코스닥
						AND A.INDEX_CODE2 IN ('001','029')  -- 001:코스피, 029:코스피200
						)
				PIVOT (
						SUM(INDEX_WEIGHT)
						FOR INDEX_NAME_KR IN ('코스피' AS "WGT_K"
											, '코스피 200' AS "WGT_K200"
											, '코스닥' AS "WGT_Q"
										)
										)
				--WHERE JNAME IN ('삼성전자', 'SK하이닉스', '알테오젠')
				ORDER BY WGT_K DESC NULLS LAST
				)
                    
            , PERIOD_RET AS (
                        SELECT 
                            STDJONG
                            , ROUND(SUM(CASE WHEN WKDATE = '{enddate}' THEN MOD_PRICE END) / SUM(CASE WHEN WKDATE = '{startdate}' THEN MOD_PRICE END)-1, 3) AS PRD_RET
                        FROM 
                            (
                            SELECT WKDATE, STDJONG, JONG, MOD_PRICE
                            FROM AMAKT.E_MA_FN_SUJUNG_PRICE
                            WHERE WKDATE IN ('{startdate}', '{enddate}')
                            )
                        GROUP BY  STDJONG
                        )

            , RAWDATA AS (
                
                SELECT     A.BASE_DT
                        , A.FUND_NM
                        , A.KOR_NM
                        , A.SEC_WGT
                        , CASE WHEN E.CLAS_CD = 'SI.002' THEN '대형주' 
                                            WHEN E.CLAS_CD = 'SI.003' THEN '중형주'
                                            WHEN E.CLAS_CD = 'SI.004' THEN '소형주'
                                            WHEN E.CLAS_CD = 'SI.201' THEN '코스닥'
                                            ELSE '미분류'            
                            END AS SEC_SIZE              
                        FROM       
                                    (  
                                        SELECT   a.BASE_DT
                                                , a.PTF_CD
                                                , a.SUB_FUND_CD
                                                , a.FUND_NM
                                                , a.ISIN_CD
                                                , a.KOR_NM
                                                , a.FUND_NAV
                                                , a.FUND_WGT*100 AS FUND_WGT
                                                , a.SEC_WGT*100 AS SEC_WGT
                                                , NVL(SUBSTR(io.CLAS_CD, 1, 3), 'W00') AS GICS_LVL1
                                                , NVL(SUBSTR(io.CLAS_CD, 1, 5), 'W0000') AS GICS_LVL2
                                                , NVL(io.CLAS_CD, 'W000000') AS GICS_LVL3
                                        FROM    (       
                                                SELECT  BASE_DT, PTF_CD, SUB_FUND_CD, FUND_NM, ISIN_CD , KOR_NM, FUND_NAV, ROUND(FUND_WGT, 8) AS FUND_WGT
                                                        , ROUND(EVL_AMT/FUND_NAV,8) AS SEC_WGT
                                                FROM    (
                                                            SELECT  e.BASE_DT,e.PTF_CD, NULL AS SUB_FUND_CD, m.KOR_NM AS FUND_NM
                                                                    , NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) AS ISIN_CD
                                                                    , NVL(sm.KOR_NM, em.KOR_NM) AS KOR_NM                              -- 선물 내 이름 사용 후 없으면 주식 이름 사용
                                                                    , n.NET_AST_TOT_AMT AS FUND_NAV, n.NET_AST_TOT_AMT/n.NET_AST_TOT_AMT AS FUND_WGT 
                                                                    , SUM(e.EVL_AMT * NVL(w.WGT, 1)) AS EVL_AMT                        -- 선물 내 비중이 있으면 곱해주고 그게 아니라면 주식이니 그냥 1 곱합
                                                            FROM    AIVSTP.FSBD_PTF_MSTR m
                                                                    INNER JOIN          AIVSTP.FSCD_PTF_ENTY_EVL_COMP e
                                                                                ON      m.PTF_CD = e.PTF_CD 
                                                                                AND     e.BASE_DT = '{enddate}'
                                                                                AND     e.DECMP_TCD =  'A'                               -- 통합펀드(PAR)의 경우에는 'A'로 분해, 개별펀드는 'E'로 분해
                                                                                AND     m.EX_TCD = e.EX_TCD                              -- KRW 
                                                                                AND     e.AST_CD  IN  ('STK')                     -- 주식과 선물 포함, 참고로 'A'로 분해시 ETF까지는 분해되어 있음
                                                                LEFT OUTER JOIN      AMAKT.FSBD_ERM_STK_MAP_MT em                 -- 삼성전자우 와 같은 종목을 삼성전자로 인식하기 위해 필요
                                                                                    ON  e.ISIN_CD = em.ISIN_CD
                                                                LEFT OUTER JOIN      AMAKT.FSBD_ERM_IDX_ENTY_WGT w                    -- 선물 분해 로직
                                                                                    ON  NVL(em.RM_ISIN_CD, e.ISIN_CD) = w.IDX_CD      -- 선물의 RM_ISIN_CD 를 활용해서 연결
                                                                                    AND e.BASE_DT = w.BASE_DT 
                                                                LEFT OUTER JOIN      AMAKT.FSBD_ERM_STK_MAP_MT sm                  -- em 테이블에서 했던 것 동일 반복
                                                                                    ON  w.ISIN_CD = sm.ISIN_CD   
                                                                    LEFT OUTER JOIN     AIVSTP.FSCD_PTF_EVL_COMP n 
                                                                                    ON  e.BASE_DT = n.BASE_DT   
                                                                                    AND m.PTF_CD = n.PTF_CD     
                                                                                    
                                                            WHERE m.PTF_CD IN ('308620' , '308614')
                                                            GROUP BY  e.BASE_DT,e.PTF_CD,  m.KOR_NM,  NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) ,  NVL(sm.KOR_NM, em.KOR_NM) , n.NET_AST_TOT_AMT 
                                                            
                                                            UNION ALL               -- 개별펀드 포함
                                                                                                    
                                                            SELECT  e.BASE_DT,k.PTF_CD, k.SUB_PTF_CD , m.KOR_NM AS FUND_NM
                                                                    , NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) AS ISIN_CD
                                                                    , NVL(sm.KOR_NM, em.KOR_NM) AS KOR_NM                             -- 선물 내 이름 사용 후 없으면 주식 이름 사용
                                                                    , n.NET_AST_TOT_AMT AS FUND_NAV , k.WGT AS FUND_WGT
                                                                    , SUM(e.EVL_AMT * NVL(w.WGT, 1)) AS EVL_AMT                        -- 선물 내 비중이 있으면 곱해주고 그게 아니라면 주식이니 그냥 1 곱합
                                                            FROM    AIVSTP.FSBD_PTF_MSTR m
                                                                    INNER JOIN      (       
                                                                                        SELECT   BASE_DT, PTF_CD, ISIN_CD AS SUB_PTF_CD , EVL_AMT , WGT 
                                                                                        FROM     AIVSTP.FSCD_PTF_ENTY_EVL_COMP e1
                                                                                        WHERE        BASE_DT = '{enddate}'
                                                                                            AND      DECMP_TCD = 'U'
                                                                                            AND      AST_CD IN ('SFD', 'FND')
                                                                                            AND      PTF_CD IN ('308620' , '308614')
                                                                                        
                                                                                            
                                                                                    ) k
                                                                            ON      m.PTF_CD = k.SUB_PTF_CD
                                                                            AND     m.KOR_NM LIKE '%주식%'
                                                                    LEFT JOIN          AIVSTP.FSCD_PTF_ENTY_EVL_COMP e
                                                                                ON      k.SUB_PTF_CD = e.PTF_CD 
                                                                                AND     e.BASE_DT = k.BASE_DT
                                                                                AND     e.DECMP_TCD =  'E' --  개별펀드는 'E'로 분해
                                                                                AND     e.AST_CD  IN  ('STK')                     -- 주식과 선물 포함, 참고로 'A'로 분해시 ETF까지는 분해되어 있음         
                                                                    LEFT OUTER JOIN     AMAKT.FSBD_ERM_STK_MAP_MT em                     -- 삼성전자우 와 같은 종목을 삼성전자로 인식하기 위해 필요
                                                                                    ON  e.ISIN_CD = em.ISIN_CD
                                                                    LEFT OUTER JOIN     AMAKT.FSBD_ERM_IDX_ENTY_WGT w                    -- 선물 분해 로직
                                                                                    ON  NVL(em.RM_ISIN_CD, e.ISIN_CD) = w.IDX_CD         -- 선물의 RM_ISIN_CD 를 활용해서 연결
                                                                                    AND e.BASE_DT = w.BASE_DT 
                                                                    LEFT OUTER JOIN     AMAKT.FSBD_ERM_STK_MAP_MT sm                     -- em 테이블에서 했던 것 동일 반복
                                                                                    ON  w.ISIN_CD = sm.ISIN_CD 
                                                                    LEFT OUTER JOIN     AIVSTP.FSCD_PTF_EVL_COMP n 
                                                                                    ON  e.BASE_DT = n.BASE_DT   
                                                                                    AND m.PTF_CD = n.PTF_CD            
                                                                                    
                                                            GROUP BY  e.BASE_DT, k.PTF_CD, k.SUB_PTF_CD,  m.KOR_NM,  NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) ,  NVL(sm.KOR_NM, em.KOR_NM) , n.NET_AST_TOT_AMT, k.WGT         
                                                        )
                                                ) a
                                                LEFT OUTER JOIN AMAKT.FSBD_ERM_STK_MAP_MT s
                                                        ON  a.ISIN_CD = s.ISIN_CD
                                                LEFT OUTER JOIN AMAKT.FSBD_ENTY_CCS_IO_MT io
                                                        ON  a.BASE_DT BETWEEN io.ST_DT AND io.END_DT 
                                                        AND NVL(s.RM_ISIN_CD, a.ISIN_CD) = io.ISIN_CD 
                                                        AND io.CCS_TCD = 'STK' 
                                                        AND io.CLAS_TYP = 'W3'
                                                LEFT OUTER JOIN AIVSTP.FSBD_CCS_MSTR c
                                                        ON io.CLAS_CD = c.CLAS_CD 
                                                        AND c.CCS_TCD = 'STK' 
                                                        AND c.CLAS_TYP = 'W'
                                
                                                ORDER BY a.PTF_CD, a.FUND_NM , a.ISIN_CD
                                            ) A
                        LEFT OUTER JOIN      AIVSTP.FSBD_CCS_MSTR B
                                        ON   B.CCS_TCD = 'STK' 
                                        AND  B.CLAS_CD = A.GICS_LVL1
                        LEFT OUTER JOIN      AIVSTP.FSBD_CCS_MSTR C
                                        ON   C.CCS_TCD = 'STK' 
                                        AND  C.CLAS_CD = A.GICS_LVL2
                        LEFT OUTER JOIN      AIVSTP.FSBD_CCS_MSTR D
                                        ON   D.CCS_TCD = 'STK' 
                                        AND  D.CLAS_CD = A.GICS_LVL3        
                        LEFT OUTER JOIN  
                                        (       
                                                SELECT  * 
                                                FROM    AMAKT.FSBD_ENTY_CCS_IO_MT
                                                WHERE  1=1
                                                AND   CCS_TCD = 'STK' 
                                                AND   CLAS_TYP = 'S'
                                        ) E
                                ON  A.ISIN_CD = E.ISIN_CD           
                                AND  A.BASE_DT   BETWEEN E.ST_DT AND E.END_DT 
                        INNER JOIN           POLSEL.V_FUND_CD  V
                                        ON   A.PTF_CD = V.AM_FUND_CD    
                                        WHERE FUND_WGT NOT LIKE '100'
                        ORDER BY  BASE_DT DESC, SEC_WGT DESC
                )
                
        , PIVOT_TABLE AS (
                    SELECT *
                    FROM ( SELECT DISTINCT FUND_NM, KOR_NM, ROUND(SEC_WGT, 2) AS SEC_WGT, SEC_SIZE
                            FROM RAWDATA
                        )
                    PIVOT (
                            SUM(SEC_WGT)
                            FOR FUND_NM IN ('투자풀ESG1호[주식]' AS "ESG1호"
                                            ,'투자풀4-3호[주식]' AS "4-3호"
                                            ,'투자풀4-5호[주식]' AS "4-5호"
                                            ,'투자풀4-6호[주식]' AS "4-6호"
                                            ,'투자풀4-7호[주식]' AS "4-7호"
                                            ,'투자풀4-8호[주식]' AS "4-8호"
                                            ,'투자풀4-9호[주식]' AS "4-9호"
                                            ,'투자풀4-10호(주식)' AS "4-10호"
                                            ,'투자풀4-11호(주식)' AS "4-11호"
                                            ,'투자풀ESG6호[주식]' AS "6호"
                                            ,'투자풀ESG7호[주식]' AS "7호")
                            )
    --					ORDER BY KOR_NM ASC
                        )
                        
    SELECT 
        A.JNAME, A.INDUSTRY_LEV1_NM AS 대분류, A.INDUSTRY_LEV2_NM AS 중분류, A.INDUSTRY_LEV3_NM AS 소분류
        , A.WGT_K AS "BM(K)"
        , A.WGT_K200 AS "BM(K200)"
        , A.WGT_Q AS "BM(KQ)"
        , A.일수익률
        , C.PRD_RET AS 기간수익률, B.SEC_SIZE
        , B."ESG1호", B."4-3호", B."4-5호", B."4-6호", B."4-7호", B."4-8호", B."4-9호", B."4-10호", B."4-11호", B."6호", B."7호"

    FROM 
        BM_WGT A
    LEFT JOIN 
        PIVOT_TABLE B
        ON A.JNAME = B.KOR_NM
    LEFT JOIN 
        PERIOD_RET C
        ON A.STDJONG = C.STDJONG
    WHERE B.KOR_NM IS NOT NULL
    ORDER BY "BM(K)" DESC NULLS LAST, "BM(KQ)" DESC

        '''.format(startdate=startdate , enddate= enddate)
    
    index_sql = '''
            WITH BM_WGT AS (
				SELECT *
				FROM (
						SELECT B.JNAME
							, B.STDJONG
							, B.INDUSTRY_LEV1_NM
							, B.INDUSTRY_LEV2_NM
							, B.INDUSTRY_LEV3_NM
							, A.INDEX_NAME_KR
							, A.INDEX_WEIGHT
							, ROUND(B.RATE/100, 4) AS "일수익률"
						
						FROM AMAKT.E_MA_KRX_PKG_CONST A
						
						LEFT JOIN AMAKT.E_MA_FN_JONGMOK_INFO B
							ON A.FILE_DATE = B.WKDATE
							AND A.CONSTITUENT_ISIN = B.STDJONG
							AND B.INDEX_ID IN ('I.001', 'I.201')
						WHERE A.FILE_DATE = '{enddate}'
						AND A.INDEX_CODE1 IN ('1', '2')  -- 1:코스피, 2:코스닥
						AND A.INDEX_CODE2 IN ('001','029')  -- 001:코스피, 029:코스피200
						)
				PIVOT (
						SUM(INDEX_WEIGHT)
						FOR INDEX_NAME_KR IN ('코스피' AS "WGT_K"
											, '코스피 200' AS "WGT_K200"
											, '코스닥' AS "WGT_Q"
										)
										)
				--WHERE JNAME IN ('삼성전자', 'SK하이닉스', '알테오젠')
				ORDER BY WGT_K DESC NULLS LAST
				)
                    
            , PERIOD_RET AS (
                        SELECT 
                            STDJONG
                            , ROUND(SUM(CASE WHEN WKDATE = '{enddate}' THEN MOD_PRICE END) / SUM(CASE WHEN WKDATE = '{startdate}' THEN MOD_PRICE END)-1, 3) AS PRD_RET
                        FROM 
                            (
                            SELECT WKDATE, STDJONG, JONG, MOD_PRICE
                            FROM AMAKT.E_MA_FN_SUJUNG_PRICE
                            WHERE WKDATE IN ('{startdate}', '{enddate}')
                            )
                        GROUP BY  STDJONG
                        )

            , RAWDATA AS (
                
                SELECT     A.BASE_DT
                        , A.FUND_NM
                        , A.KOR_NM
                        , A.SEC_WGT
                        , CASE WHEN E.CLAS_CD = 'SI.002' THEN '대형주' 
                                            WHEN E.CLAS_CD = 'SI.003' THEN '중형주'
                                            WHEN E.CLAS_CD = 'SI.004' THEN '소형주'
                                            WHEN E.CLAS_CD = 'SI.201' THEN '코스닥'
                                            ELSE '미분류'            
                            END AS SEC_SIZE              
                        FROM       
                                    (  
                                        SELECT   a.BASE_DT
                                                , a.PTF_CD
                                                , a.SUB_FUND_CD
                                                , a.FUND_NM
                                                , a.ISIN_CD
                                                , a.KOR_NM
                                                , a.FUND_NAV
                                                , a.FUND_WGT*100 AS FUND_WGT
                                                , a.SEC_WGT*100 AS SEC_WGT
                                                , NVL(SUBSTR(io.CLAS_CD, 1, 3), 'W00') AS GICS_LVL1
                                                , NVL(SUBSTR(io.CLAS_CD, 1, 5), 'W0000') AS GICS_LVL2
                                                , NVL(io.CLAS_CD, 'W000000') AS GICS_LVL3
                                        FROM    (       
                                                SELECT  BASE_DT, PTF_CD, SUB_FUND_CD, FUND_NM, ISIN_CD , KOR_NM, FUND_NAV, ROUND(FUND_WGT, 8) AS FUND_WGT
                                                        , ROUND(EVL_AMT/FUND_NAV,8) AS SEC_WGT
                                                FROM    (
                                                            SELECT  e.BASE_DT,e.PTF_CD, NULL AS SUB_FUND_CD, m.KOR_NM AS FUND_NM
                                                                    , NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) AS ISIN_CD
                                                                    , NVL(sm.KOR_NM, em.KOR_NM) AS KOR_NM                              -- 선물 내 이름 사용 후 없으면 주식 이름 사용
                                                                    , n.NET_AST_TOT_AMT AS FUND_NAV, n.NET_AST_TOT_AMT/n.NET_AST_TOT_AMT AS FUND_WGT 
                                                                    , SUM(e.EVL_AMT * NVL(w.WGT, 1)) AS EVL_AMT                        -- 선물 내 비중이 있으면 곱해주고 그게 아니라면 주식이니 그냥 1 곱합
                                                            FROM    AIVSTP.FSBD_PTF_MSTR m
                                                                    INNER JOIN          AIVSTP.FSCD_PTF_ENTY_EVL_COMP e
                                                                                ON      m.PTF_CD = e.PTF_CD 
                                                                                AND     e.BASE_DT = '{enddate}'
                                                                                AND     e.DECMP_TCD =  'A'                               -- 통합펀드(PAR)의 경우에는 'A'로 분해, 개별펀드는 'E'로 분해
                                                                                AND     m.EX_TCD = e.EX_TCD                              -- KRW 
                                                                                AND     e.AST_CD  IN  ('STK')                     -- 주식과 선물 포함, 참고로 'A'로 분해시 ETF까지는 분해되어 있음
                                                                LEFT OUTER JOIN      AMAKT.FSBD_ERM_STK_MAP_MT em                 -- 삼성전자우 와 같은 종목을 삼성전자로 인식하기 위해 필요
                                                                                    ON  e.ISIN_CD = em.ISIN_CD
                                                                LEFT OUTER JOIN      AMAKT.FSBD_ERM_IDX_ENTY_WGT w                    -- 선물 분해 로직
                                                                                    ON  NVL(em.RM_ISIN_CD, e.ISIN_CD) = w.IDX_CD      -- 선물의 RM_ISIN_CD 를 활용해서 연결
                                                                                    AND e.BASE_DT = w.BASE_DT 
                                                                LEFT OUTER JOIN      AMAKT.FSBD_ERM_STK_MAP_MT sm                  -- em 테이블에서 했던 것 동일 반복
                                                                                    ON  w.ISIN_CD = sm.ISIN_CD   
                                                                    LEFT OUTER JOIN     AIVSTP.FSCD_PTF_EVL_COMP n 
                                                                                    ON  e.BASE_DT = n.BASE_DT   
                                                                                    AND m.PTF_CD = n.PTF_CD     
                                                                                    
                                                            WHERE m.PTF_CD IN ('308620' , '308614')
                                                            GROUP BY  e.BASE_DT,e.PTF_CD,  m.KOR_NM,  NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) ,  NVL(sm.KOR_NM, em.KOR_NM) , n.NET_AST_TOT_AMT 
                                                            
                                                            UNION ALL               -- 개별펀드 포함
                                                                                                    
                                                            SELECT  e.BASE_DT,k.PTF_CD, k.SUB_PTF_CD , m.KOR_NM AS FUND_NM
                                                                    , NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) AS ISIN_CD
                                                                    , NVL(sm.KOR_NM, em.KOR_NM) AS KOR_NM                             -- 선물 내 이름 사용 후 없으면 주식 이름 사용
                                                                    , n.NET_AST_TOT_AMT AS FUND_NAV , k.WGT AS FUND_WGT
                                                                    , SUM(e.EVL_AMT * NVL(w.WGT, 1)) AS EVL_AMT                        -- 선물 내 비중이 있으면 곱해주고 그게 아니라면 주식이니 그냥 1 곱합
                                                            FROM    AIVSTP.FSBD_PTF_MSTR m
                                                                    INNER JOIN      (       
                                                                                        SELECT   BASE_DT, PTF_CD, ISIN_CD AS SUB_PTF_CD , EVL_AMT , WGT 
                                                                                        FROM     AIVSTP.FSCD_PTF_ENTY_EVL_COMP e1
                                                                                        WHERE        BASE_DT = '{enddate}'
                                                                                            AND      DECMP_TCD = 'U'
                                                                                            AND      AST_CD IN ('SFD', 'FND')
                                                                                            AND      PTF_CD IN ('308620' , '308611')
                                                                                        
                                                                                            
                                                                                    ) k
                                                                            ON      m.PTF_CD = k.SUB_PTF_CD
                                                                            AND     m.KOR_NM LIKE '%주식%'
                                                                    LEFT JOIN          AIVSTP.FSCD_PTF_ENTY_EVL_COMP e
                                                                                ON      k.SUB_PTF_CD = e.PTF_CD 
                                                                                AND     e.BASE_DT = k.BASE_DT
                                                                                AND     e.DECMP_TCD =  'E' --  개별펀드는 'E'로 분해
                                                                                AND     e.AST_CD  IN  ('STK')                     -- 주식과 선물 포함, 참고로 'A'로 분해시 ETF까지는 분해되어 있음         
                                                                    LEFT OUTER JOIN     AMAKT.FSBD_ERM_STK_MAP_MT em                     -- 삼성전자우 와 같은 종목을 삼성전자로 인식하기 위해 필요
                                                                                    ON  e.ISIN_CD = em.ISIN_CD
                                                                    LEFT OUTER JOIN     AMAKT.FSBD_ERM_IDX_ENTY_WGT w                    -- 선물 분해 로직
                                                                                    ON  NVL(em.RM_ISIN_CD, e.ISIN_CD) = w.IDX_CD         -- 선물의 RM_ISIN_CD 를 활용해서 연결
                                                                                    AND e.BASE_DT = w.BASE_DT 
                                                                    LEFT OUTER JOIN     AMAKT.FSBD_ERM_STK_MAP_MT sm                     -- em 테이블에서 했던 것 동일 반복
                                                                                    ON  w.ISIN_CD = sm.ISIN_CD 
                                                                    LEFT OUTER JOIN     AIVSTP.FSCD_PTF_EVL_COMP n 
                                                                                    ON  e.BASE_DT = n.BASE_DT   
                                                                                    AND m.PTF_CD = n.PTF_CD            
                                                                                    
                                                            GROUP BY  e.BASE_DT, k.PTF_CD, k.SUB_PTF_CD,  m.KOR_NM,  NVL(sm.RM_ISIN_CD, NVL(w.ISIN_CD, NVL(em.RM_ISIN_CD, e.ISIN_CD))) ,  NVL(sm.KOR_NM, em.KOR_NM) , n.NET_AST_TOT_AMT, k.WGT         
                                                        )
                                                ) a
                                                LEFT OUTER JOIN AMAKT.FSBD_ERM_STK_MAP_MT s
                                                        ON  a.ISIN_CD = s.ISIN_CD
                                                LEFT OUTER JOIN AMAKT.FSBD_ENTY_CCS_IO_MT io
                                                        ON  a.BASE_DT BETWEEN io.ST_DT AND io.END_DT 
                                                        AND NVL(s.RM_ISIN_CD, a.ISIN_CD) = io.ISIN_CD 
                                                        AND io.CCS_TCD = 'STK' 
                                                        AND io.CLAS_TYP = 'W3'
                                                LEFT OUTER JOIN AIVSTP.FSBD_CCS_MSTR c
                                                        ON io.CLAS_CD = c.CLAS_CD 
                                                        AND c.CCS_TCD = 'STK' 
                                                        AND c.CLAS_TYP = 'W'
                                
                                                ORDER BY a.PTF_CD, a.FUND_NM , a.ISIN_CD
                                            ) A
                        LEFT OUTER JOIN      AIVSTP.FSBD_CCS_MSTR B
                                        ON   B.CCS_TCD = 'STK' 
                                        AND  B.CLAS_CD = A.GICS_LVL1
                        LEFT OUTER JOIN      AIVSTP.FSBD_CCS_MSTR C
                                        ON   C.CCS_TCD = 'STK' 
                                        AND  C.CLAS_CD = A.GICS_LVL2
                        LEFT OUTER JOIN      AIVSTP.FSBD_CCS_MSTR D
                                        ON   D.CCS_TCD = 'STK' 
                                        AND  D.CLAS_CD = A.GICS_LVL3        
                        LEFT OUTER JOIN  
                                        (       
                                                SELECT  * 
                                                FROM    AMAKT.FSBD_ENTY_CCS_IO_MT
                                                WHERE  1=1
                                                AND   CCS_TCD = 'STK' 
                                                AND   CLAS_TYP = 'S'
                                        ) E
                                ON  A.ISIN_CD = E.ISIN_CD           
                                AND  A.BASE_DT   BETWEEN E.ST_DT AND E.END_DT 
                        INNER JOIN           POLSEL.V_FUND_CD  V
                                        ON   A.PTF_CD = V.AM_FUND_CD    
                                        WHERE FUND_WGT NOT LIKE '100'
                        ORDER BY  BASE_DT DESC, SEC_WGT DESC
                )
                
        , PIVOT_TABLE AS (
				SELECT *
				FROM ( SELECT DISTINCT FUND_NM, KOR_NM, ROUND(SEC_WGT, 2) AS SEC_WGT, SEC_SIZE
						FROM RAWDATA
					)
				PIVOT (
						SUM(SEC_WGT)
						FOR FUND_NM IN ('투자풀인덱스2-3호(주식)' AS "인덱스2-3호"
										,'투자풀인덱스2-4호[주식]' AS "인덱스2-4호"
										,'투자풀인덱스2-6호[주식]' AS "인덱스2-6호"
										,'투자풀인덱스2-7호[주식]' AS "인덱스2-7호"
										,'투자풀인덱스2-8호[주식]' AS "인덱스2-8호"
										,'투자풀인덱스4-4호[주식]' AS "인덱스4-4호"
										,'투자풀인덱스3-3호[주식]' AS "인덱스3-3호"
										,'투자풀인덱스3-4호(주식)' AS "인덱스3-4호"
										,'투자풀인덱스3-5호[주식]' AS "인덱스3-5호"
										,'투자풀인덱스3-6호[주식]' AS "인덱스3-6호"
										,'투자풀ESG인덱스2호[주식]' AS "인덱스2호")
						)
					)
					
        SELECT 
            A.JNAME, A.INDUSTRY_LEV1_NM AS 대분류, A.INDUSTRY_LEV2_NM AS 중분류, A.INDUSTRY_LEV3_NM AS 소분류
            , A.WGT_K AS "BM(K)"
            , A.WGT_K200 AS "BM(K200)"
            , A.WGT_Q AS "BM(KQ)"
            , A.일수익률
            , C.PRD_RET AS 기간수익률, B.SEC_SIZE
            , B."인덱스2-3호", B."인덱스2-4호", B."인덱스2-6호", B."인덱스2-7호", B."인덱스2-8호", B."인덱스4-4호", B."인덱스3-3호", B."인덱스3-4호", B."인덱스3-5호", B."인덱스3-6호", B."인덱스2호"
        FROM 
            BM_WGT A
        LEFT JOIN 
            PIVOT_TABLE B
            ON A.JNAME = B.KOR_NM
        LEFT JOIN 
            PERIOD_RET C
            ON A.STDJONG = C.STDJONG
        WHERE B.KOR_NM IS NOT NULL
        ORDER BY "BM(K)" DESC NULLS LAST, "BM(KQ)" DESC

        '''.format(startdate=startdate , enddate= enddate)
    
    if type_fund == '액티브(Active)':
        df = db_connect(url, active_sql)
    else:
        df = db_connect(url, index_sql)

    df = df.set_index('JNAME').fillna(0)
    df.index.name = '종목명'
    
    
    market_df = db_connect(url, mkt_index_sql).set_index('WKDATE')
    kosdaq_ret, kospi_ret, k200_ret = market_df.sort_values(by="WKDATE").groupby("KFNAME").apply(calculate_period_return)
    kosdaq_index, kospi_index, k200_index = market_df.sort_values(by='WKDATE').groupby('KFNAME').last()['CLOSE_INDEX']

    pivot_market_df = market_df.pivot_table(index='WKDATE', columns='KFNAME', values='CLOSE_INDEX').sort_index(ascending=True)

    daily_return = pivot_market_df.pct_change()
    cumul_return = pd.DataFrame(np.round(np.cumprod(1+daily_return)-1, 2))



    tab1, tab2 = st.tabs(['현황', '기여도'])
        
    # df = pd.read_excel('contribution.xlsx', header=1, sheet_name='Sheet1').iloc[:, 1:].set_index('JNAME').fillna(0)
    fund_list = list(df.columns[9:])
    active_corp_list = ['마이다스', '브이아이','BNK','안다','베어링','이스트','한투밸류','우리','한투신탁','하나','키움']
    index_corp_list = ['교보','KB','iM','유리','NH','DB','DB_k200','KB_k200','iM_k200','NH_k200','유리_k200']

    columns_to_select = list(df.columns[3:5]) + fund_list
    
    

    if type_fund == '액티브(Active)':
        corp_list = active_corp_list
    else:
        corp_list = index_corp_list


    dict_corp_name = dict()
    for x, y in zip(fund_list, corp_list):
        case = {x: y}
        dict_corp_name.update(case)


    bm_kospi = df['BM(K)']
    bm_k200= df['BM(K200)']
    bm_kosdaq = df['BM(KQ)']

    period_ret = df['기간수익률']

    if type_fund == '액티브(Active)':
        ctb_k = np.round((df[fund_list[:-2]].subtract(bm_kospi.values, axis=0)).multiply(period_ret.values, axis=0) * 100, 2)
        ctb_k200 = np.round((df[fund_list[-2:]].subtract(bm_k200.values, axis=0)).multiply(period_ret.values, axis=0) * 100, 2)
    else:
        ctb_k = np.round((df[fund_list[:-5]].subtract(bm_kospi.values, axis=0)).multiply(period_ret.values, axis=0) * 100, 2)
        ctb_k200 = np.round((df[fund_list[-5:]].subtract(bm_k200.values, axis=0)).multiply(period_ret.values, axis=0) * 100, 2)

    ctb_df = pd.concat([df['대분류'], ctb_k, ctb_k200], axis=1)
    ctb_df.columns = ['대분류'] + corp_list
    
    ctb_df_sector = ctb_df.groupby('대분류').sum()

    ctb_df_sector.columns = corp_list

    rank_ctb_sector = pd.DataFrame(ctb_df_sector.sum(axis=0).sort_values(ascending=False), columns=['기여도 합(bp)'])
    rank_ctb_sector.rename(index=dict_corp_name, inplace=True)
    rank_ctb_sector.index.name = '운용사'

    
    with tab1:
        st.subheader('Market Index')
        col1, col2, col3 = st.columns(3)

        col1.metric('KOSPI', f'{kospi_index}', delta=f'{kospi_ret}%')
        col2.metric('KOSPI200', f'{k200_index}', delta=f'{k200_ret}%')
        col3.metric('KOSDAQ', f'{kosdaq_index}', delta=f'{kosdaq_ret}%')

        st.divider()

        st.subheader('개별펀드 포트폴리오 현황')
        _, bc = st.columns([9.3, 0.7])
        bc.download_button("Get Data", data=df.to_csv().encode('cp949'), file_name="Fund_Portfolio.csv")
        st.dataframe(df, height=400, use_container_width=True)
        
        st.divider()

        col1, col2 = st.columns([6, 4])
        col1.subheader('섹터별 비중')
        wgt_by_sector = df.groupby('대분류').sum()[columns_to_select]
        col1.dataframe(wgt_by_sector, height=390, use_container_width=True)


        col2.subheader('BM 대비 비중')
        option = col2.selectbox('Funds', fund_list)           
        if option not in ['6호','7호','인덱스3-3호','인덱스3-4호','인덱스3-5호','인덱스3-6호','인덱스2호']:
            temp = wgt_by_sector[option] - wgt_by_sector['BM(K)']
        else:
            temp = wgt_by_sector[option] - wgt_by_sector['BM(K200)']
        temp = pd.DataFrame(temp, columns=['차이']).reset_index()
        
        
        chart_temp = alt.Chart(temp).mark_bar().encode(
            x = alt.X('차이:Q', axis=alt.Axis(title='BM 대비(%p)', grid=False)),
            y = alt.Y('대분류:N', axis=alt.Axis(title='섹터', grid=False), sort='-x'),
            color = alt.Color(
                        '차이', 
                        scale=alt.Scale(scheme='blues'),
                        legend=None
                    )
        ).properties(height=330)
        col2.altair_chart(chart_temp, use_container_width=True)

    with tab2:
        st.subheader('종목별 수익률 기여도')
        st.dataframe(ctb_df.style.format(precision=2), height=300, use_container_width=True)

    
        col1, col2 = st.columns([8, 2])

        with col1:
            st.subheader('업종별 수익률 기여도')
            st.dataframe(ctb_df_sector.style.highlight_max(axis=0, color='#C9E6F0').highlight_min(axis=0, color='#FFE3E3').format(precision=2), height=385, use_container_width=True)
            # st.dataframe(ctb_df_sector.style.background_gradient(axis=None, cmap='YlOrBr').format(precision=2), height=385, use_container_width=True)
            
        with col2:
            st.subheader('Top Funds')
            st.dataframe(rank_ctb_sector, height=425, use_container_width=True)

        # temp = rank_ctb_sector.copy()
        # temp = temp.reset_index()
        # color_scale = alt.Scale(scheme='blues')
        
        # alt_chart = alt.Chart(temp).mark_bar().encode(
        #     y = alt.Y('운용사').sort('-x'),
        #     x = alt.X('기여도 합(bp)'),
        #     color = alt.Color('운용사', scale=color_scale, legend=None)
        # ).properties(height=350)


if __name__ == '__main__' :
    main()

