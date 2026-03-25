import { useState, useEffect, useCallback } from 'react';
import { ResponsiveLine } from '@nivo/line';
import { Search, Clock, Zap, Users, AlertCircle } from 'lucide-react';
import { fetchTimePattern, fetchGoldenTime } from '../../utils/network.js';
import LoadingOverlay from '../../components/LoadingOverlay.jsx';
import styles from './HourlyPattern.module.css';

// ── 시간대 정렬 ────────────────────────────────────────────────
const TIME_ORDER = [
  '05~06','06~07','07~08','08~09','09~10','10~11',
  '11~12','12~13','13~14','14~15','15~16','16~17',
  '17~18','18~19','19~20','20~21','21~22','22~23','23~24','24~'
];

const sortByTime = (arr) =>
  [...arr].sort((a, b) => TIME_ORDER.indexOf(a['시간대']) - TIME_ORDER.indexOf(b['시간대']));

// ── 차트 테마 ──────────────────────────────────────────────────
const CHART_THEME = {
  background: 'transparent',
  textColor: '#8fa3c4',
  fontSize: 11,
  fontFamily: "'DM Sans', sans-serif",
  axis: {
    domain: { line: { stroke: '#1f2d47', strokeWidth: 1 } },
    ticks: { line: { stroke: '#1f2d47' }, text: { fill: '#8fa3c4', fontSize: 11 } },
    legend: { text: { fill: '#4a6080', fontSize: 11 } },
  },
  grid: { line: { stroke: '#1f2d47', strokeWidth: 1 } },
  crosshair: { line: { stroke: '#3b82f6', strokeWidth: 1, strokeOpacity: 0.5 } },
  tooltip: {
    container: {
      background: '#141b2d',
      color: '#e8f0fe',
      fontSize: 12,
      borderRadius: 6,
      border: '1px solid #1f2d47',
      padding: '8px 12px',
    },
  },
};

const YEARS = ['2021','2020','2019','2018','2017','2016','2015'];

export default function HourlyPattern() {
  const [year, setYear] = useState('2021');
  const [station, setStation] = useState('강남');
  const [inputVal, setInputVal] = useState('강남');
  const [dayType, setDayType] = useState('평일');
  const [chartData, setChartData] = useState([]);
  const [goldenTime, setGoldenTime] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    if (!station.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const [patternRes, goldenRes] = await Promise.all([
        fetchTimePattern(year, station, dayType),
        fetchGoldenTime(year, station, dayType),
      ]);

      // ── 패턴 데이터 → Nivo Line 형식 변환 ────────────────
      // API: [{시간대, 구분, 평균인원, 혼잡도지수}, ...]
      // Nivo: [{id: '승차', data: [{x:'05~06', y: 123}, ...]}]
      const raw = patternRes?.result ?? [];
      const grouped = {};
      for (const row of raw) {
        const g = row['구분'];
        if (!grouped[g]) grouped[g] = [];
        grouped[g].push({ x: row['시간대'], y: row['평균인원'], 혼잡도: row['혼잡도지수'] });
      }
      const lines = Object.entries(grouped).map(([id, pts]) => ({
        id,
        data: sortByTime(pts.map(p => ({ ...p }))).map(p => ({ x: p.x, y: p.y, 혼잡도: p.혼잡도 })),
      }));
      setChartData(lines);

      // ── 골든타임 ──────────────────────────────────────────
      setGoldenTime(goldenRes?.result ?? []);
    } catch (e) {
      setError('데이터를 불러오지 못했습니다. API 서버를 확인해주세요.');
    } finally {
      setLoading(false);
    }
  }, [year, station, dayType]);

  useEffect(() => { load(); }, [load]);

  const handleSearch = () => {
    setStation(inputVal.trim());
  };

  const peakIcon = (idx) => {
    const icons = ['🥇','🥈','🥉'];
    return icons[idx] ?? '';
  };

  return (
    <div className={styles.page}>
      {/* ── 컨트롤 바 ─────────────────────────────────────── */}
      <div className={styles.controls}>
        <div className={styles.searchBox}>
          <Search size={14} className={styles.searchIcon} />
          <input
            className={styles.searchInput}
            value={inputVal}
            onChange={e => setInputVal(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSearch()}
            placeholder="역명 입력 (예: 강남, 홍대입구)"
          />
          <button className={styles.searchBtn} onClick={handleSearch}>검색</button>
        </div>

        <div className={styles.filterGroup}>
          {YEARS.map(y => (
            <button
              key={y}
              className={`${styles.filterBtn} ${year === y ? styles.active : ''}`}
              onClick={() => setYear(y)}
            >{y}</button>
          ))}
        </div>

        <div className={styles.filterGroup}>
          {['평일','주말'].map(d => (
            <button
              key={d}
              className={`${styles.filterBtn} ${dayType === d ? styles.active : ''}`}
              onClick={() => setDayType(d)}
            >{d}</button>
          ))}
        </div>
      </div>

      {/* ── 골든타임 카드 ──────────────────────────────────── */}
      {goldenTime.length > 0 && (
        <div className={styles.goldenRow}>
          <div className={styles.goldenLabel}>
            <Zap size={13} />
            <span>골든 타임 (혼잡도 TOP 3)</span>
          </div>
          {goldenTime.map((g, i) => (
            <div key={g['시간대']} className={styles.goldenCard}>
              <span className={styles.goldenRank}>{peakIcon(i)}</span>
              <span className={styles.goldenTime}>{g['시간대']}</span>
              <span className={styles.goldenValue}>
                <Users size={11} /> {Math.round(g['총인원']).toLocaleString()}명
              </span>
              <span className={styles.goldenCongestion}>
                혼잡도 {g['최대혼잡도']}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* ── 메인 차트 ──────────────────────────────────────── */}
      <div className={styles.chartCard}>
        <div className={styles.cardHeader}>
          <Clock size={14} />
          <span className={styles.cardTitle}>
            {station} · {year}년 · {dayType} — 시간대별 평균 승하차 인원
          </span>
          <span className={styles.cardNote}>혼잡도 지수 = (시간대 평균 / 전체 시간대 평균) × 100</span>
        </div>

        <div className={styles.chartArea} style={{ position: 'relative' }}>
          {loading && <LoadingOverlay message="데이터 집계 중..." />}
          {error && (
            <div className={styles.errorBox}>
              <AlertCircle size={16} />
              <span>{error}</span>
            </div>
          )}
          {!loading && !error && chartData.length === 0 && (
            <div className={styles.empty}>조회된 데이터가 없습니다.</div>
          )}
          {!loading && chartData.length > 0 && (
            <ResponsiveLine
              data={chartData}
              theme={CHART_THEME}
              margin={{ top: 20, right: 140, bottom: 60, left: 70 }}
              xScale={{ type: 'point' }}
              yScale={{ type: 'linear', min: 'auto', max: 'auto', stacked: false }}
              curve="monotoneX"
              axisBottom={{
                tickSize: 4,
                tickPadding: 8,
                tickRotation: -35,
                legend: '시간대',
                legendOffset: 50,
                legendPosition: 'middle',
              }}
              axisLeft={{
                tickSize: 4,
                tickPadding: 8,
                legend: '평균 인원 (명)',
                legendOffset: -58,
                legendPosition: 'middle',
                format: v => v >= 1000 ? `${(v/1000).toFixed(0)}k` : v,
              }}
              colors={['#3b82f6', '#10d9a0']}
              lineWidth={2}
              pointSize={5}
              pointColor={{ theme: 'background' }}
              pointBorderWidth={2}
              pointBorderColor={{ from: 'serieColor' }}
              enableArea
              areaOpacity={0.08}
              useMesh
              legends={[{
                anchor: 'right',
                direction: 'column',
                justify: false,
                translateX: 130,
                translateY: 0,
                itemWidth: 120,
                itemHeight: 22,
                itemTextColor: '#8fa3c4',
                symbolSize: 10,
                symbolShape: 'circle',
                effects: [{ on: 'hover', style: { itemTextColor: '#e8f0fe' } }],
              }]}
              tooltip={({ point }) => (
                <div style={{
                  background:'#141b2d', border:'1px solid #1f2d47',
                  borderRadius:6, padding:'8px 12px', fontSize:12, color:'#e8f0fe'
                }}>
                  <strong style={{ color: point.serieColor }}>{point.serieId}</strong>
                  <div>시간대: {point.data.xFormatted}</div>
                  <div>평균 인원: <strong>{Math.round(point.data.y).toLocaleString()}명</strong></div>
                  {point.data.혼잡도 !== undefined && (
                    <div>혼잡도 지수: <strong>{point.data.혼잡도}</strong></div>
                  )}
                </div>
              )}
            />
          )}
        </div>
      </div>
    </div>
  );
}
