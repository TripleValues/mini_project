import { TrendingUp } from 'lucide-react';
import '@styles/Placeholder.css';

const YearlyTrend =() => {
  return (
    <div className="page">
      <div className="placeholder">
        <TrendingUp size={40} className="icon" />
        <span className="label">연도별 이용객 추이 시각화</span>
        <span className="sub">
          2008–2021년 서울 지하철 전체 성장 흐름을<br/>
          영역 차트(Area Chart)로 시각화합니다.
        </span>
        <div className="metaRow">
          <span className="badge">METRO-01</span>
          <span className="badge">FEAT-01</span>
          <span className="badge">준비 중</span>
        </div>
      </div>
    </div>
  );
}

export default YearlyTrend;