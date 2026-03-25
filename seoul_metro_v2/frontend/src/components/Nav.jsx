import { NavLink } from 'react-router-dom'
import {
  TrendingUp, BarChart2, Clock, Grid, Calendar, Train
} from 'lucide-react'
import '@styles/Nav.css'

const MENU = [
  { path: '/yearly',   label: '연도별 추이',       sub: 'METRO-01', Icon: TrendingUp  },
  { path: '/stations', label: '역별 혼잡도',        sub: 'METRO-02', Icon: BarChart2   },
  { path: '/hourly',   label: '시간대별 패턴',      sub: 'METRO-03', Icon: Clock       },
  { path: '/behavior', label: '요일별 이용행태',    sub: 'METRO-04', Icon: Grid        },
  { path: '/seasonal', label: '월별 성수기 분석',   sub: 'METRO-05', Icon: Calendar    },
];

const Nav = () => {
  return (
    <nav className="nav">
      <div className="logo">
        <Train size={20} className="logoIcon" />
        <div>
          <span className="logoMain">SEOUL</span>
          <span className="logoSub">METRO ANALYTICS</span>
        </div>
      </div>

      <div className="divider" />

      <ul className="menu">
        {MENU.map(({ path, label, sub, Icon }) => (
          <li key={path}>
            <NavLink
              to={path}
              className={({ isActive }) =>
                `"item" ${isActive ? "active" : ""}`
              }
            >
              <Icon size={16} className="icon" />
              <div className="labels">
                <span className="labelMain">{label}</span>
                <span className="labelSub">{sub}</span>
              </div>
              <div className="activeLine" />
            </NavLink>
          </li>
        ))}
      </ul>

      <div className="footer">
        <span className="footerText">Seoul Metro Open Data</span>
        <span className="footerText">2008 – 2021</span>
      </div>
    </nav>
  );
}

export default Nav;