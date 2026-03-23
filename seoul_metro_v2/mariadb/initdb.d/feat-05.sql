-- 계절 및 분기 데이터 뷰 테이블 생성
CREATE OR REPLACE TABLE `feat_05` AS
SELECT
  `날짜`, `역번호`, `역명`, `구분`,
  `05~06`, `06~07`, `07~08`, `08~09`, `09~10`, `10~11`, `11~12`,
	`12~13`, `13~14`, `14~15`, `15~16`, `16~17`, `17~18`, `18~19`,
	`19~20`, `20~21`, `21~22`, `22~23`, `23~24`, `24~`,
	QUARTER(`날짜`) AS `분기`,
  CONCAT(YEAR(`날짜`), '년 ', QUARTER(`날짜`), '분기') AS `년도분기`,
  CASE
    WHEN MONTH(`날짜`) IN (3,4,5) THEN '봄'
    WHEN MONTH(`날짜`) IN (6,7,8) THEN '여름'
    WHEN MONTH(`날짜`) IN (9,10,11) THEN '가을'
    ELSE '겨울'
  END AS `계절`,
  `요일`,
  (
    IFNULL(`05~06`,0) + IFNULL(`06~07`,0) + IFNULL(`07~08`,0) +
    IFNULL(`08~09`,0) + IFNULL(`09~10`,0) + IFNULL(`10~11`,0) +
    IFNULL(`11~12`,0)
  ) AS `AM`,
  (
    IFNULL(`12~13`,0) + IFNULL(`13~14`,0) + IFNULL(`14~15`,0) +
    IFNULL(`15~16`,0) + IFNULL(`16~17`,0) + IFNULL(`17~18`,0) +
    IFNULL(`18~19`,0) + IFNULL(`19~20`,0) + IFNULL(`20~21`,0) +
    IFNULL(`21~22`,0) + IFNULL(`22~23`,0) + IFNULL(`23~24`,0) +
    IFNULL(`24~`,0)
  ) AS `PM`,
  (
    IFNULL(`05~06`,0) + IFNULL(`06~07`,0) + IFNULL(`07~08`,0) +
    IFNULL(`08~09`,0) + IFNULL(`09~10`,0) + IFNULL(`10~11`,0) +
    IFNULL(`11~12`,0) + IFNULL(`12~13`,0) + IFNULL(`13~14`,0) + 
    IFNULL(`14~15`,0) + IFNULL(`15~16`,0) + IFNULL(`16~17`,0) + 
    IFNULL(`17~18`,0) + IFNULL(`18~19`,0) + IFNULL(`19~20`,0) + 
    IFNULL(`20~21`,0) + IFNULL(`21~22`,0) + IFNULL(`22~23`,0) + 
    IFNULL(`23~24`,0) + IFNULL(`24~`,0)
  ) AS `총 이용객 수`
FROM `seoul_metro`;

-- feat_05
CREATE INDEX idx_station ON feat_05(역번호);


-- 계절 및 승하차별 변동성
SELECT
	sq.`날짜`, d.`요일`, sq.`구분`,
	SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
	SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
	SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
	SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
	SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
	SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
	SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
	SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
	SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
	SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
FROM feat_05 AS sq
LEFT JOIN `요일` AS d
	ON sq.`요일` = d.`코드`
LEFT JOIN metro_line AS ml
	ON sq.`역번호` = ml.역번호
WHERE YEAR(`날짜`) = 2008
  AND `계절` = '봄'
GROUP BY sq.`날짜`, d.`요일`, sq.`구분`;

-- 순수 계절 및 년도별 변동성
SELECT
	YEAR(`날짜`) AS `년도`,
	SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
	SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
	SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
	SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
	SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
	SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
	SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
	SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
	SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
	SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
FROM feat_05 AS sq
LEFT JOIN `요일` AS d
	ON sq.`요일` = d.`코드`
LEFT JOIN metro_line AS ml
	ON sq.`역번호` = ml.역번호	
WHERE YEAR(`날짜`) = 2020
  AND `계절` = '겨울'
GROUP BY `년도`;

-- 계절 및 년월별 변동성
SELECT
	YEAR(`날짜`) AS `년도`, MONTH(sq.`날짜`) AS `월`,
	SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
	SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
	SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
	SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
	SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
	SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
	SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
	SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
	SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
	SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
FROM feat_05 AS sq
LEFT JOIN `요일` AS d
	ON sq.`요일` = d.`코드`
LEFT JOIN metro_line AS ml
	ON sq.`역번호` = ml.역번호	
WHERE YEAR(`날짜`) = 2020
  AND `계절` = '겨울'
GROUP BY `년도`, `월`;

-- 계절 및 일별 변동성
SELECT
	`날짜`,
	SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
	SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
	SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
	SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
	SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
	SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
	SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
	SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
	SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
	SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
FROM feat_05 AS sq
LEFT JOIN `요일` AS d
	ON sq.`요일` = d.`코드`
LEFT JOIN metro_line AS ml
	ON sq.`역번호` = ml.역번호	
WHERE YEAR(`날짜`) = 2020
  AND `계절` = '겨울'
GROUP BY `날짜`;


-- 월 및 승하차별 변동성
SELECT
	sq.`날짜`, d.`요일`, sq.`구분`,
	SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
	SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
	SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
	SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
	SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
	SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
	SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
	SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
	SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
	SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
FROM feat_05 AS sq
LEFT JOIN `요일` AS d
	ON sq.`요일` = d.`코드`
LEFT JOIN metro_line AS ml
	ON sq.`역번호` = ml.역번호
WHERE YEAR(`날짜`) = 2008
  AND MONTH(`날짜`) = 05
GROUP BY sq.`날짜`, d.`요일`, sq.`구분`;

-- 월별 변동성
SELECT
	sq.`날짜`, d.`요일`,
	SUM(sq.`05~06`) AS `05~06`, SUM(sq.`06~07`) AS `06~07`, 
	SUM(sq.`07~08`) AS `07~08`, SUM(sq.`08~09`) AS `08~09`, 
	SUM(sq.`09~10`) AS `09~10`, SUM(sq.`10~11`) AS `10~11`, 
	SUM(sq.`11~12`) AS `11~12`, SUM(sq.`12~13`) AS `12~13`, 
	SUM(sq.`13~14`) AS `13~14`, SUM(sq.`14~15`) AS `14~15`, 
	SUM(sq.`15~16`) AS `15~16`, SUM(sq.`16~17`) AS `16~17`, 
	SUM(sq.`17~18`) AS `17~18`, SUM(sq.`18~19`) AS `18~19`, 
	SUM(sq.`19~20`) AS `19~20`, SUM(sq.`20~21`) AS `20~21`, 
	SUM(sq.`21~22`) AS `21~22`, SUM(sq.`22~23`) AS `22~23`, 
	SUM(sq.`23~24`) AS `23~24`, SUM(sq.`24~`) AS `24~`
FROM feat_05 AS sq
LEFT JOIN `요일` AS d
	ON sq.`요일` = d.`코드`
LEFT JOIN metro_line AS ml
	ON sq.`역번호` = ml.역번호
WHERE YEAR(`날짜`) = 2008
  AND MONTH(`날짜`) = 05
GROUP BY sq.`날짜`, d.`요일`;
