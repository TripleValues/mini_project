import { useState, useEffect, useCallback } from 'react';
import { ResponsiveHeatMap } from '@nivo/heatmap';
import { BarChart2, List, X, AlertCircle, TrendingUp, Moon } from 'lucide-react';
import { fetchHeatmap, fetchClickRanking } from '@utils/network.js';
import LoadingOverlay from '@components/LoadingOverlay.jsx';
import '@styles/DayBehavior.css';

// ── 차트 테마 ──────────────────────────────────────────────────
const CHART_THEME = {
  background: 'transparent',
  textColor: '#8fa3c4',
  fontSize: 11,
  fontFamily: "'DM Sans', sans-serif",
  axis: {
    ticks: { text: { fill: '#8fa3c4', fontSize: 11 } },
  },
  tooltip: {
    container: {
      background: '#141b2d',
      color: '#e8f0fe',
      fontSize: 12,
      borderRadius: 6,
      border: '1px solid #1f2d47',
    },
  },
};

const YEARS = ['2021','2020','2019','2018','2017','2016','2015'];
const ON_OFF = ['전체','승차','하차'];
const MODES = [
  { value: 'total', label: '전체 이용객', Icon: TrendingUp },
  { value: 'night', label: '심야 급증', Icon: Moon },
];

// ── 히트맵 색상 범위 ───────────────────────────────────────────
const HEAT_COLORS = [
  { value: 0,   color: '#0a1628' },
  { value: 0.2, color: '#0d3060' },
  { value: 0.4, color: '#0f4c8a' },
  { value: 0.6, color: '#1a65b8' },
  { value: 0.8, color: '#2980e8' },
  { value: 1,   color: '#4da3ff' },
];

const DayBehavior = () => {
  const [year, setYear]       = useState('2021');
  const [onOff, setOnOff]     = useState('전체');
  const [heatData, setHeatData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(null);

  // ── 클릭 사이드바 상태 ─────────────────────────────────────
  const [sidebar, setSidebar] = useState(null); // { line, dayName }
  const [rankData, setRankData] = useState([]);
  const [rankMode, setRankMode] = useState('total');
  const [rankLoading, setRankLoading] = useState(false);

  // ── 히트맵 데이터 로드 ────────────────────────────────────
  const loadHeatmap = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // API: { result: [{ id: '2호선', data: [{x:'월요일', y:합계인원, 평균이용객수, 심야이용객수}] }] }
      const res = await fetchHeatmap(year, onOff);
      setHeatData(res?.result ?? []);
    } catch {
      setError('히트맵 데이터를 불러오지 못했습니다.');
    } finally {
      setLoading(false);
    }
  }, [year, onOff]);

  useEffect(() => { loadHeatmap(); }, [loadHeatmap]);

  // ── 클릭 → 사이드바 랭킹 로드 ────────────────────────────
  const loadRanking = useCallback(async (line, dayName, mode) => {
    setRankLoading(true);
    try {
      const res = await fetchClickRanking(year, line, dayName, mode, onOff);
      setRankData(res?.result ?? []);
    } catch {
      setRankData([]);
    } finally {
      setRankLoading(false);
    }
  }, [year, onOff]);

  const handleCellClick = (cell) => {
    // Nivo heatmap cell: cell.serieId = 호선(Y축), cell.data.x = 요일(X축)
    const line    = cell.serieId;
    const dayName = cell.data.x;
    setSidebar({ line, dayName });
    loadRanking(line, dayName, rankMode);
  };

  const handleModeChange = (mode) => {
    setRankMode(mode);
    if (sidebar) loadRanking(sidebar.line, sidebar.dayName, mode);
  };

  // ── 랭킹 최대값 (막대 너비 비율용) ─────────────────────────
  const rankMax = rankData.length > 0
    ? Math.max(...rankData.map(r => r['총이용량'] ?? r['심야이용량'] ?? 0))
    : 1;

  const rankValueKey = rankMode === 'total' ? '총이용량' : '심야이용량';

  return (
    <div className="page">
      {/* ── 컨트롤 바 ─────────────────────────────────────── */}
      <div className="controls">
        <div className="filterGroup">
          {YEARS.map(y => (
            <button
              key={y}
              className={`filterBtn ${year === y ? 'active' : ''}`}
              onClick={() => setYear(y)}
            >{y}</button>
          ))}
        </div>

        <div className="filterGroup">
          {ON_OFF.map(o => (
            <button
              key={o}
              className={`filterBtn ${onOff === o ? 'activeGreen' : ''}`}
              onClick={() => setOnOff(o)}
            >{o}</button>
          ))}
        </div>

        <span className="hint">
          셀 클릭 시 해당 노선·요일의 역별 랭킹을 확인할 수 있습니다
        </span>
      </div>

      {/* ── 본문 레이아웃 ──────────────────────────────────── */}
      <div className={`"layout" ${sidebar ? "withSidebar" : ""}`}>
        {/* ── 히트맵 카드 ─────────────────────────────────── */}
        <div className="chartCard">
          <div className="cardHeader">
            <BarChart2 size={14} />
            <span className="cardTitle">
              {year}년 · {onOff} — 호선별 요일 이용객 히트맵
            </span>
            <span className="cardNote">Y축: 호선 | X축: 요일 | 색상: 합계 인원</span>
          </div>

          <div className="chartArea" style={{ position: 'relative' }}>
            {loading && <LoadingOverlay message="히트맵 집계 중..." />}
            {error && (
              <div className="errorBox"><AlertCircle size={16} /><span>{error}</span></div>
            )}
            {!loading && !error && heatData.length === 0 && (
              <div className="empty">조회된 데이터가 없습니다.</div>
            )}
            {!loading && heatData.length > 0 && (
              <ResponsiveHeatMap
                data={heatData}
                theme={CHART_THEME}
                margin={{ top: 16, right: 30, bottom: 60, left: 80 }}
                // Nivo HeatMap: data[].id = Y축 (호선), data[].data[].x = X축 (요일)
                valueFormat=">-.2s"
                axisBottom={{
                  tickSize: 4,
                  tickPadding: 8,
                  tickRotation: -15,
                  legend: '요일',
                  legendOffset: 50,
                  legendPosition: 'middle',
                }}
                axisLeft={{
                  tickSize: 4,
                  tickPadding: 8,
                  legend: '호선',
                  legendOffset: -68,
                  legendPosition: 'middle',
                }}
                colors={{
                  type: 'sequential',
                  scheme: 'blues',
                  minValue: 0,
                }}
                emptyColor="#0e1420"
                borderRadius={3}
                borderWidth={2}
                borderColor="#080c12"
                labelTextColor={{ from: 'color', modifiers: [['brighter', 3]] }}
                annotations={[]}
                onClick={handleCellClick}
                hoverTarget="cell"
                cellHoverOthersOpacity={0.5}
                tooltip={({ cell }) => (
                  <div style={{
                    background:'#141b2d', border:'1px solid #1f2d47',
                    borderRadius:6, padding:'10px 14px', fontSize:12, color:'#e8f0fe'
                  }}>
                    <div style={{ fontWeight:700, marginBottom:4 }}>
                      {cell.serieId} · {cell.data.x}
                    </div>
                    <div>합계 인원: <strong>{Math.round(cell.value).toLocaleString()}명</strong></div>
                    {cell.data['평균이용객수'] != null && (
                      <div>평균 이용객: {Math.round(cell.data['평균이용객수']).toLocaleString()}명</div>
                    )}
                    {cell.data['심야이용객수'] != null && (
                      <div>심야 이용객: {Math.round(cell.data['심야이용객수']).toLocaleString()}명</div>
                    )}
                    <div style={{ color:'#4a6080', marginTop:4, fontSize:10 }}>클릭하여 역별 랭킹 보기</div>
                  </div>
                )}
              />
            )}
          </div>
        </div>

        {/* ── 사이드바 랭킹 ──────────────────────────────── */}
        {sidebar && (
          <div className="sidebar">
            <div className="sideHeader">
              <div>
                <span className="sideTitle">
                  <List size={13} /> 역별 이용객 랭킹
                </span>
                <span className="sideSub">
                  {sidebar.line} · {sidebar.dayName} · TOP 20
                </span>
              </div>
              <button className="closeBtn" onClick={() => setSidebar(null)}>
                <X size={14} />
              </button>
            </div>

            <div className="modeToggle">
              {MODES.map(({ value, label, Icon }) => (
                <button
                  key={value}
                  className={`"modeBtn" ${rankMode === value ? "modeActive" : ""}`}
                  onClick={() => handleModeChange(value)}
                >
                  <Icon size={11} /> {label}
                </button>
              ))}
            </div>

            <div className="rankList" style={{ position: 'relative' }}>
              {rankLoading && <LoadingOverlay message="랭킹 집계 중..." />}
              {!rankLoading && rankData.length === 0 && (
                <div className="empty">데이터가 없습니다.</div>
              )}
              {!rankLoading && rankData.map((r, i) => {
                const val = r[rankValueKey] ?? 0;
                const pct = rankMax > 0 ? (val / rankMax) * 100 : 0;
                const구분 = r['구분'];
                return (
                  <div key={`${r['역명']}-${구분}-${i}`} className="rankItem">
                    <span className="rankNo">{i + 1}</span>
                    <div className="rankInfo">
                      <div className="rankName">
                        {r['역명']}
                        {구분 && <span className="rankTag">{구분}</span>}
                      </div>
                      <div className="rankBar">
                        <div className="rankBarFill" style={{ width: `${pct}%` }} />
                      </div>
                    </div>
                    <span className="rankVal">
                      {Math.round(val).toLocaleString()}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default DayBehavior;