import { Calendar } from 'lucide-react';
import '@styles/Placeholder.css';

const Seasonality = () => {
  return (
    <div className="page">
      <div className="placeholder">
        <Calendar size={40} className="icon" />
        <span className="label">월별 성수기/비수기 지수 분석</span>
        <span className="sub">
          계절·공휴일 영향 분석 및 연간 최대 이용 월을<br/>
          콤보 차트 + 캘린더 히트맵으로 시각화합니다.
        </span>
        <div className="metaRow">
          <span className="badge">METRO-05</span>
          <span className="badge">FEAT-05</span>
          <span className="badge">작업 중</span>
        </div>
      </div>
    </div>
  );
}

export default Seasonality;