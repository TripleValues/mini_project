import { BarChart2 } from 'lucide-react';
import '@styles/Placeholder.css';

const TopStations = () => {
  return (
    <div className="page">
      <div className="placeholder">
        <BarChart2 size={40} className="icon" />
        <span className="label">역별 혼잡도 TOP N 랭킹</span>
        <span className="sub">
          이용객 수 상위 10/20/50 역을<br/>
          가로 막대 차트(Horizontal Bar Chart)로 시각화합니다.
        </span>
        <div className="metaRow">
          <span className="badge">METRO-02</span>
          <span className="badge">FEAT-02</span>
          <span className="badge">준비 중</span>
        </div>
      </div>
    </div>
  );
}

export default TopStations;