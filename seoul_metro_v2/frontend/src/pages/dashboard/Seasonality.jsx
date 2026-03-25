import { Calendar } from 'lucide-react';
import styles from './Placeholder.module.css';

export default function Seasonality() {
  return (
    <div className={styles.page}>
      <div className={styles.placeholder}>
        <Calendar size={40} className={styles.icon} />
        <span className={styles.label}>월별 성수기/비수기 지수 분석</span>
        <span className={styles.sub}>
          계절·공휴일 영향 분석 및 연간 최대 이용 월을<br/>
          콤보 차트 + 캘린더 히트맵으로 시각화합니다.
        </span>
        <div className={styles.metaRow}>
          <span className={styles.badge}>METRO-05</span>
          <span className={styles.badge}>FEAT-05</span>
          <span className={styles.badge}>작업 중</span>
        </div>
      </div>
    </div>
  );
}
