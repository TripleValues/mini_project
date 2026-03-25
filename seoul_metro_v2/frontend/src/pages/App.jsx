import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Nav from '../components/Nav.jsx';
import Header from '../components/Header.jsx';
import YearlyTrend   from './dashboard/YearlyTrend.jsx';
import TopStations   from './dashboard/TopStations.jsx';
import HourlyPattern from './dashboard/HourlyPattern.jsx';
import DayBehavior   from './dashboard/DayBehavior.jsx';
import Seasonality   from './dashboard/Seasonality.jsx';
import styles from './App.module.css';

export default function App() {
  return (
    <BrowserRouter>
      <div className={styles.shell}>
        <Nav />
        <div className={styles.main}>
          <Header />
          <main className={styles.content}>
            <Routes>
              <Route path="/" element={<Navigate to="/behavior" replace />} />
              <Route path="/yearly"   element={<YearlyTrend />} />
              <Route path="/stations" element={<TopStations />} />
              <Route path="/hourly"   element={<HourlyPattern />} />
              <Route path="/behavior" element={<DayBehavior />} />
              <Route path="/seasonal" element={<Seasonality />} />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  );
}
