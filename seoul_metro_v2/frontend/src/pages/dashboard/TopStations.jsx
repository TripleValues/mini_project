import React, { useEffect, useState, useCallback } from 'react';
import axios from 'axios';
import { ResponsiveBar } from '@nivo/bar';
import { BarChart2, Loader2, Award, Zap, Calendar, Search } from 'lucide-react';
import styles from '@styles/Placeholder.module.css';

export default function TopStations() {
  const [chartData, setChartData] = useState([]);
  const [kpi, setKpi] = useState(null);
  const [loading, setLoading] = useState(true);
  
  // 1. 실제 API 호출에 사용될 "확정된" 상태
  const [appliedFilters, setAppliedFilters] = useState({
    topN: 10,
    type: 'all',
    date: { year: 2021, month: 5, day: 15 }
  });

  // 2. UI에서 실시간으로 변하는 "임시" 상태 (조회 버튼 누르기 전)
  const [tempFilters, setTempFilters] = useState({
    topN: 10,
    type: 'all',
    date: { year: 2021, month: 5, day: 15 }
  });

  const years = Array.from({ length: 2021 - 2008 + 1 }, (_, i) => 2008 + i).reverse();
  const months = Array.from({ length: 12 }, (_, i) => i + 1);
  const days = Array.from({ length: 31 }, (_, i) => i + 1);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      const { topN, type, date } = appliedFilters;
      
      const [chartRes, kpiRes] = await Promise.all([
        axios.get(`http://localhost:8000/feat_02/metro_02_01`, {
          params: { top_n: topN, type, target_year: date.year, target_month: date.month, target_day: date.day }
        }),
        axios.get(`http://localhost:8000/feat_02/metro_02_02`, {
          params: { type, target_year: date.year, target_month: date.month, target_day: date.day }
        })
      ]);

      if (chartRes.data.status === "success") setChartData(chartRes.data.chart_data.reverse());
      if (kpiRes.data.status === "success") setKpi(kpiRes.data.kpi);
    } catch (error) {
      console.error("데이터 로드 실패:", error);
    } finally {
      setLoading(false);
    }
  }, [appliedFilters]);

  // 첫 렌더링 및 appliedFilters 변경 시에만 실행
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // [조회하기] 버튼 클릭 핸들러
  const handleApply = () => {
    setAppliedFilters(tempFilters);
  };

  const handleDateChange = (e) => {
    const { name, value } = e.target;
    setTempFilters(prev => ({
      ...prev,
      date: { ...prev.date, [name]: Number(value) }
    }));
  };

  return (
    <div className={styles.page}>
      {/* 1. 필터 컨트롤러 영역 */}
      <div className={styles.header} style={{ marginBottom: '20px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '15px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <BarChart2 size={28} color="#6366f1" />
            <h2 className={styles.label}>역별 혼잡도 TOP {appliedFilters.topN} 랭킹</h2>
          </div>

          <div style={{ display: 'flex', gap: '12px', alignItems: 'center', background: '#fff', padding: '12px', borderRadius: '12px', boxShadow: '0 2px 8px rgba(0,0,0,0.05)',color : '#000000' }}>
            {/* 날짜 선택 */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', borderRight: '1px solid #eee', paddingRight: '12px',color : '#000000' }}>
              <Calendar size={18} color="#6366f1" />
              <select name="year" value={tempFilters.date.year} onChange={handleDateChange} style={selectStyle}>
                {years.map(y => <option key={y} value={y}>{y}년</option>)}
              </select>
              <select name="month" value={tempFilters.date.month} onChange={handleDateChange} style={selectStyle}>
                {months.map(m => <option key={m} value={m}>{m}월</option>)}
              </select>
              <select name="day" value={tempFilters.date.day} onChange={handleDateChange} style={selectStyle}>
                {days.map(d => <option key={d} value={d}>{d}일</option>)}
              </select>
            </div>

            {/* Top N 및 구분 */}
            <select value={tempFilters.topN} onChange={(e) => setTempFilters(p => ({...p, topN: Number(e.target.value)}))} style={selectStyle}>
              <option value={10}>TOP 10</option>
              <option value={20}>TOP 20</option>
            </select>
            <select value={tempFilters.type} onChange={(e) => setTempFilters(p => ({...p, type: e.target.value}))} style={selectStyle}>
              <option value="all">전체</option>
              <option value="승차">승차</option>
              <option value="하차">하차</option>
            </select>

            {/* 조회 버튼 */}
            <button 
              onClick={handleApply}
              style={{ 
                display: 'flex', alignItems: 'center', gap: '6px', backgroundColor: '#6366f1', color: '#fff', 
                border: 'none', padding: '8px 16px', borderRadius: '8px', fontWeight: '600', cursor: 'pointer' 
              }}
            >
              <Search size={16} /> 조회하기
            </button>
          </div>
        </div>
      </div>

      {/* 2. KPI 카드 섹션 */}
      {kpi && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px', marginBottom: '20px' }}>
          <div style={{ padding: '20px', background: '#eef2ff', borderRadius: '12px', borderLeft: '5px solid #6366f1' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#4338ca', marginBottom: '10px' }}>
              <Award size={20} /> <span style={{ fontWeight: '600' }}>최대 혼잡 역</span>
            </div>
            <div style={{ fontSize: '24px', fontWeight: 'bold',color : '#000000' }}>
              {kpi.today_top.station_name} <span style={{ fontSize: '16px', fontWeight: 'normal', color: '#666' }}>({kpi.today_top.value.toLocaleString()}명)</span>
            </div>
          </div>
          <div style={{ padding: '20px', background: '#fff7ed', borderRadius: '12px', borderLeft: '5px solid #f59e0b' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#9a3412', marginBottom: '10px' }}>
              <Zap size={20} /> <span style={{ fontWeight: '600' }}>데이터 격차</span>
            </div>
            <div style={{ fontSize: '24px', fontWeight: 'bold',color : '#000000' }}>
              약 {kpi.scale_insight.ratio}배 <span style={{ fontSize: '14px', fontWeight: 'normal', color: '#666' }}>({kpi.scale_insight.bottom_station} 대비)</span>
            </div>
          </div>
        </div>
      )}

      {/* 3. 가로 막대 차트 영역 */}
      <div style={{ height: '550px', background: '#fff', padding: '20px', borderRadius: '12px', boxShadow: '0 4px 15px rgba(0,0,0,0.05)' }}>
        {loading ? (
          <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <Loader2 className="animate-spin" /> <span style={{marginLeft: '10px'}}>빅데이터 분석 중...</span>
          </div>
        ) : (
          <ResponsiveBar
            data={chartData}
            keys={['total']}
            indexBy="역명"
            margin={{ top: 10, right: 40, bottom: 50, left: 120 }}
            padding={0.35}
            layout="horizontal"
            colors="#6366f1"
            borderRadius={6}
            enableGridX={true}
            enableGridY={false}
            labelSkipWidth={12}
            labelTextColor="#ffffff"
            labelFormat={v => `${(v / 1000).toFixed(1)}k`}
            axisLeft={{
              tickSize: 0,
              tickPadding: 10,
            }}
            axisBottom={{
              format: v => `${(v / 1000).toLocaleString()}k`,
              legend: '이용객 수 (단위: 천 명)',
              legendPosition: 'middle',
              legendOffset: 40
            }}
            theme={{
              axis: {
                ticks: { text: { fontSize: 13, fontWeight: 500, fill: '#444' } },
                legend: { text: { fontSize: 14, fontWeight: 600, fill: '#888' } }
              },
              grid: { line: { stroke: '#f0f0f0', strokeWidth: 1 } }
            }}
            tooltip={({ value, indexValue }) => (
              <div style={{ padding: '10px', background: '#2d3436', color: '#fff', borderRadius: '6px', fontSize: '13px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)' }}>
                <strong style={{ color: '#a29bfe' }}>{indexValue}</strong><br/>
                총 이용객: {value.toLocaleString()}명
              </div>
            )}
            animate={true}
            motionConfig="gentle"
          />
        )}
      </div>
    </div>
  );
}

const selectStyle = {
  padding: '8px 12px',
  borderRadius: '8px',
  border: '1px solid #052753',
  fontSize: '14px',
  backgroundColor: '#ffffff',
  outline: 'none',
  cursor: 'pointer',
  transition: 'border-color 0.2s',
  color : '#000000'
};