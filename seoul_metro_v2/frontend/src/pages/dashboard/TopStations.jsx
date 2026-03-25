import { BarChart2 } from 'lucide-react';
import styles from '@styles/Placeholder.module.css';

export default function TopStations() {
  return (
    <div className={styles.page}>
      <div className={styles.placeholder}>
        <BarChart2 size={40} className={styles.icon} />
        <span className={styles.label}>역별 혼잡도 TOP N 랭킹</span>
        <span className={styles.sub}>
          이용객 수 상위 10/20/50 역을<br/>
          가로 막대 차트(Horizontal Bar Chart)로 시각화합니다.
        </span>
        <div className={styles.metaRow}>
          <span className={styles.badge}>METRO-02</span>
          <span className={styles.badge}>FEAT-02</span>
          <span className={styles.badge}>준비 중</span>
        </div>
      </div>
    </div>
  );
}
