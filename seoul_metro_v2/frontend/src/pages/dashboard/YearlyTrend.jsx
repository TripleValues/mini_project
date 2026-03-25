import { TrendingUp } from 'lucide-react';
import styles from '@styles/Placeholder.module.css';

export default function YearlyTrend() {
  return (
    <div className={styles.page}>
      <div className={styles.placeholder}>
        <TrendingUp size={40} className={styles.icon} />
        <span className={styles.label}>연도별 이용객 추이 시각화</span>
        <span className={styles.sub}>
          2008–2021년 서울 지하철 전체 성장 흐름을<br/>
          영역 차트(Area Chart)로 시각화합니다.
        </span>
        <div className={styles.metaRow}>
          <span className={styles.badge}>METRO-01</span>
          <span className={styles.badge}>FEAT-01</span>
          <span className={styles.badge}>준비 중</span>
        </div>
      </div>
    </div>
  );
}
