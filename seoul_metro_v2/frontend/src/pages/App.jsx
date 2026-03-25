import { Routes, Route, Navigate } from 'react-router-dom';
import '@styles/App.css';
import Nav from '@components/Nav.jsx';
import Header from '@components/Header.jsx';
import YearlyTrend   from '@pages/dashboard/YearlyTrend.jsx';
import TopStations   from '@pages/dashboard/TopStations.jsx';
import HourlyPattern from '@pages/dashboard/HourlyPattern.jsx';
import DayBehavior   from '@pages/dashboard/DayBehavior.jsx';
import Seasonality   from '@pages/dashboard/Seasonality.jsx';

const App = () => {
  return (
      <div className="app-shell">
        <Nav />
        <div className="app-main">
          <Header />
          <main className="app-content">
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
  );
}

export default App;
