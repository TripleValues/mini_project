import React, { useEffect, useState, useMemo } from 'react';
import { api } from '@utils/network.js';
import { ResponsiveLine } from '@nivo/line';
import { TrendingUp, Loader2, Users, Activity, Scale, ChevronLeft } from 'lucide-react';
import styles from '@styles/Placeholder.module.css';

const YearlyTrend = () => {
  const [rawData, setRawData] = useState([]);
  const [kpiData, setKpiData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedType, setSelectedType] = useState('전체');
  const [viewMode, setViewMode] = useState('yearly');
  const [targetYear, setTargetYear] = useState(null);

  const fetchData = async () => {
    try {
      setLoading(true);
      const response = await api.get(`/feat_01/metro_01_total`, {
        params: { type: selectedType }
      });
      const result = response.data || response;
      if (result && result.status === "success") {
        setRawData(result.main_chart || []);
        setKpiData(result);
      }
    } catch (error) {
      console.error("데이터 로드 실패:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [selectedType]);

  const currentKPI = useMemo(() => {
    if (!kpiData || !kpiData.kpi_growth || kpiData.kpi_growth.length === 0) {
      return { total: 0, growth: 0, balance: 0 };
    }
    const target = viewMode === 'monthly'
      ? kpiData.kpi_growth.filter(d => d.년도 === parseInt(targetYear))
      : [kpiData.kpi_growth[kpiData.kpi_growth.length - 1]];

    const balanceTarget = viewMode === 'monthly'
      ? kpiData.kpi_balance.filter(d => d.년도 === parseInt(targetYear))
      : [kpiData.kpi_balance[0]];

    return {
      total: target.reduce((sum, d) => sum + (Number(d.total) || 0), 0),
      growth: target[target.length - 1]?.growth_rate || 0,
      balance: balanceTarget[0]?.balance_ratio || 0
    };
  }, [kpiData, viewMode, targetYear]);

  // ⭐ 그래프가 안 나오는 문제를 해결하기 위한 핵심 로직 (y값 숫자 변환)
  const chartData = useMemo(() => {
    if (!rawData || rawData.length === 0) return [];

    if (viewMode === 'yearly') {
      const yearlyMap = rawData.reduce((acc, item) => {
        const year = item.label.split('-')[0];
        // display_value를 확실하게 숫자로 변환합니다.
        const value = Number(item.display_value) || 0;
        acc[year] = (acc[year] || 0) + value;
        return acc;
      }, {});

      return [{
        id: `연간 ${selectedType}`,
        data: Object.keys(yearlyMap).sort().map(year => ({
          x: year,
          y: yearlyMap[year] // 변환된 숫자값
        }))
      }];
    } else {
      const monthlyData = rawData
        .filter(item => item.label.startsWith(targetYear))
        .map(item => ({
          x: item.label.split('-')[1] + "월",
          y: Number(item.display_value) || 0 // 변환된 숫자값
        }));
      return [{ id: `${targetYear}년 상세`, data: monthlyData }];
    }
  }, [rawData, viewMode, targetYear, selectedType]);

  return (
    <div className={styles.page} style={{ paddingTop: '80px', paddingLeft: '30px', paddingRight: '30px' }}>
      {/* 1. 헤더 영역 */}
      <div className={styles.header} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <TrendingUp size={30} color="#8884d8" />
          <h2 className={styles.label} style={{ fontSize: '20px', fontWeight: 'bold', color: '#000' }}>
            {viewMode === 'yearly' ? "연도별 이용객 거시 추이" : `${targetYear}년 월별 상세 분석`}
          </h2>
          {viewMode === 'monthly' && (
            <button 
              onClick={() => setViewMode('yearly')} 
              style={{ cursor: 'pointer', border: '1px solid #ddd', background: '#fff', padding: '6px 12px', borderRadius: '6px', display: 'flex', alignItems: 'center', color:'#333', fontSize: '14px', marginLeft: '10px' }}
            >
              <ChevronLeft size={16} /> 돌아가기
            </button>
          )}
        </div>
        <div className={styles.filterGroup}>
          {['전체', '승차', '하차'].map((t) => (
            <button 
              key={t} 
              onClick={() => setSelectedType(t)} 
              style={{ padding: '8px 18px', backgroundColor: selectedType === t ? '#8884d8' : '#fff', color: selectedType === t ? '#fff' : '#666', border: '1px solid #ddd', borderRadius: '20px', cursor: 'pointer', marginLeft: '8px', fontWeight: '500' }}
            >
              {t}
            </button>
          ))}
        </div>
      </div>

      {/* 2. KPI 카드 섹션 */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '20px', marginBottom: '30px' }}>
        <div style={{ padding: '24px', background: '#fff', borderRadius: '12px', boxShadow: '0 2px 12px rgba(0,0,0,0.06)', borderLeft: '6px solid #8884d8' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#666', fontSize: '14px', marginBottom: '10px' }}>
            <Users size={18} /> 연간 총 이용객
          </div>
          <div style={{ fontSize: '26px', fontWeight: 'bold', color:'#000' }}>
            {currentKPI.total.toLocaleString()} <span style={{fontSize: '15px', fontWeight: 'normal'}}>명</span>
          </div>
        </div>

        <div style={{ padding: '24px', background: '#fff', borderRadius: '12px', boxShadow: '0 2px 12px rgba(0,0,0,0.06)', borderLeft: '6px solid #ff9f43' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#666', fontSize: '14px', marginBottom: '10px' }}>
            <Activity size={18} /> 전년 대비 성장률
          </div>
          <div style={{ fontSize: '26px', fontWeight: 'bold', color: currentKPI.growth >= 0 ? '#ef4444' : '#3b82f6' }}>
            {currentKPI.growth}%
          </div>
        </div>

        <div style={{ padding: '24px', background: '#fff', borderRadius: '12px', boxShadow: '0 2px 12px rgba(0,0,0,0.06)', borderLeft: '6px solid #10b981' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#666', fontSize: '14px', marginBottom: '10px' }}>
            <Scale size={18} /> 승하차 균형도
          </div>
          <div style={{ fontSize: '26px', fontWeight: 'bold', color: '#000' }}>
            {currentKPI.balance} <span style={{fontSize: '15px', fontWeight: 'normal'}}>%</span>
          </div>
        </div>
      </div>

      {/* 3. 메인 차트 영역 */}
      <div style={{ background: '#fff', padding: '32px', borderRadius: '20px', boxShadow: '0 10px 30px rgba(0,0,0,0.04)', height: '520px', position: 'relative' }}>
        {loading ? (
          <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', height: '100%', gap: '12px' }}>
            <Loader2 className="animate-spin" size={40} color="#5e5ce6" />
            <span style={{ color: '#8e8e93', fontSize: '15px' }}>서버에서 데이터를 분석하고 있습니다...</span>
          </div>
        ) : chartData.length > 0 && chartData[0].data.length > 0 ? (
          <ResponsiveLine
            data={chartData}
            margin={{ top: 20, right: 40, bottom: 80, left: 80 }}
            xScale={{ type: 'point' }}
            yScale={{ type: 'linear', min: 'auto', max: 'auto', stacked: false }}
            axisBottom={{
              legend: viewMode === 'yearly' ? '연도별 추이 (클릭하여 상세 보기)' : '월별 상세 데이터',
              legendOffset: 50,
              legendPosition: 'middle',
              tickSize: 0,
              tickPadding: 15
            }}
            axisLeft={{
              format: v => `${(v / 1000000).toFixed(1)}M`,
              legend: '이용객수 (백만 명)',
              legendOffset: -65,
              legendPosition: 'middle',
              tickSize: 0,
              tickPadding: 10
            }}
            enableGridX={false}
            gridYValues={5}
            enableArea={true}
            areaOpacity={0.08}
            useMesh={true}
            colors={viewMode === 'yearly' ? ['#5e5ce6'] : ['#34c759']}
            pointSize={12}
            pointColor="#ffffff"
            pointBorderWidth={3}
            pointBorderColor={{ from: 'serieColor' }}
            onClick={(node) => {
              if (viewMode === 'yearly') {
                setTargetYear(node.data.x);
                setViewMode('monthly');
              }
            }}
            theme={{
              axis: {
                ticks: { text: { fill: '#8e8e93', fontSize: 12 } },
                legend: { text: { fill: '#1c1c1e', fontWeight: 600, fontSize: 13 } }
              },
              grid: { line: { stroke: '#f2f2f7', strokeWidth: 1 } }
            }}
          />
        ) : (
          <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%', color: '#d1d1d6', fontSize: '16px' }}>
            표시할 데이터가 존재하지 않습니다.
          </div>
        )}
      </div>
    </div>
  );
}

export default YearlyTrend
