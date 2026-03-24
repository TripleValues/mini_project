import React, { useState } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap-icons/font/bootstrap-icons.css';

const MetroDashboard = () => {
  // 상태 관리: 현재 선택된 분석 주제 및 필터
  const [activeMenu, setActiveMenu] = useState('yearly-trend');

  return (
    <div className="d-flex" style={{ minHeight: '100vh', backgroundColor: '#f4f7f6' }}>
      
      {/* [1] Sidebar: 분석 주제 전환 메뉴 */}
      <aside className="bg-dark text-white d-none d-lg-block" style={{ width: '280px', flexShrink: 0 }}>
        <div className="p-4 border-bottom border-secondary">
          <h4 className="text-primary fw-bold mb-0">
            <i className="bi bi-subway me-2"></i>METRO ANALYTICS
          </h4>
        </div>
        <div className="p-3">
          <small className="text-uppercase text-secondary fw-bold">Analysis Themes</small>
          <ul className="nav nav-pills flex-column mt-3">
            <li className="nav-item mb-2">
              <button className={`nav-link w-100 text-start text-white ${activeMenu === 'yearly-trend' ? 'active' : ''}`} onClick={() => setActiveMenu('yearly-trend')}>
                <i className="bi bi-graph-up me-2"></i> 연도별 이용객 추이
              </button>
            </li>
            <li className="nav-item mb-2">
              <button className={`nav-link w-100 text-start text-white ${activeMenu === 'top-stations' ? 'active' : ''}`} onClick={() => setActiveMenu('top-stations')}>
                <i className="bi bi-bar-chart-steps me-2"></i> 역별 혼잡도 TOP 10/20/50
              </button>
            </li>
            <li className="nav-item mb-2">
              <button className={`nav-link w-100 text-start text-white ${activeMenu === 'hourly-pattern' ? 'active' : ''}`} onClick={() => setActiveMenu('hourly-pattern')}>
                <i className="bi bi-clock-history me-2"></i> 시간대별 승하차 패턴
              </button>
            </li>
            <li className="nav-item mb-2">
              <button className={`nav-link w-100 text-start text-white ${activeMenu === 'day-behavior' ? 'active' : ''}`} onClick={() => setActiveMenu('day-behavior')}>
                <i className="bi bi-calendar-week me-2"></i> 요일별 이용 행태 분석
              </button>
            </li>
            <li className="nav-item mb-2">
              <button className={`nav-link w-100 text-start text-white ${activeMenu === 'seasonality' ? 'active' : ''}`} onClick={() => setActiveMenu('seasonality')}>
                <i className="bi bi-snow2 me-2"></i> 연도별 성수기/비수기 분석
              </button>
            </li>
          </ul>
        </div>
      </aside>

      {/* [2] Main Content Area */}
      <main className="flex-grow-1 overflow-auto">
        
        {/* Header: 내비게이션 및 사용자 정보 */}
        <header className="navbar sticky-top bg-white border-bottom px-4 py-3 shadow-sm">
          <div className="container-fluid p-0">
            <span className="navbar-brand fw-bold text-dark">Dashboard Overview</span>
            <div className="d-flex align-items-center gap-3">
              <div className="input-group d-none d-md-flex">
                <span className="input-group-text bg-light border-0"><i className="bi bi-search"></i></span>
                <input type="text" className="form-control bg-light border-0" placeholder="Search data..." />
              </div>
              <i className="bi bi-bell fs-5 text-secondary cursor-pointer"></i>
              <div className="vr mx-2"></div>
              <span className="fw-semibold">Admin. User</span>
            </div>
          </div>
        </header>

        {/* Dashboard Body */}
        <div className="container-fluid p-4">
          
          {/* [3] Filter Section */}
          <section className="card border-0 shadow-sm mb-4">
            <div className="card-body d-flex flex-wrap gap-3 align-items-center">
              <div className="d-flex align-items-center">
                <label className="me-2 text-muted fw-bold small">YEAR</label>
                <select className="form-select form-select-sm border-0 bg-light" style={{ width: '120px' }}>
                  <option>2008 ~ 2021</option>
                  <option>2021</option>
                  <option>2020</option>
                </select>
              </div>
              <div className="d-flex align-items-center">
                <label className="me-2 text-muted fw-bold small">LINE</label>
                <select className="form-select form-select-sm border-0 bg-light" style={{ width: '120px' }}>
                  <option>전체 호선</option>
                  <option>1호선</option>
                  <option>2호선</option>
                </select>
              </div>
              <div className="ms-md-auto d-flex gap-2">
                <button className="btn btn-sm btn-outline-primary"><i className="bi bi-download me-1"></i> CSV</button>
                <button className="btn btn-sm btn-primary">적용하기</button>
              </div>
            </div>
          </section>

          {/* [4] Summary Cards */}
          <section className="row mb-4">
            {['총 이용객수', '평균 승차인원', '평균 하차인원', '최대 혼잡 시간대'].map((title, idx) => (
              <div key={idx} className="col-12 col-sm-6 col-xl-3 mb-3 mb-xl-0">
                <div className="card border-0 shadow-sm h-100">
                  <div className="card-body">
                    <div className="d-flex justify-content-between align-items-start mb-3">
                      <div className="bg-primary bg-opacity-10 p-2 rounded text-primary">
                        <i className={`bi bi-${['people', 'box-arrow-in-right', 'box-arrow-right', 'alarm'][idx]} fs-4`}></i>
                      </div>
                      <span className="badge bg-success bg-opacity-10 text-success fw-normal">+2.5%</span>
                    </div>
                    <h6 className="card-subtitle text-muted mb-1">{title}</h6>
                    <h3 className="card-title fw-bold mb-0">
                      {idx === 3 ? '08:00' : (Math.floor(Math.random() * 1000000)).toLocaleString()}
                    </h3>
                  </div>
                </div>
              </div>
            ))}
          </section>

          {/* [5] Chart Grid */}
          <section className="row">
            {/* Main Trend Chart */}
            <div className="col-12 mb-4">
              <ChartWrapper title="연도별 승하차 이용객 추이 분석" description="2008년부터 2021년까지의 전체 승하차 트렌드 시계열 데이터">
                <div className="d-flex align-items-center justify-content-center h-100 text-muted border border-dashed rounded bg-light" style={{ minHeight: '350px' }}>
                  <div className="text-center">
                    <i className="bi bi-graph-up fs-1 mb-2 d-block"></i>
                    Line Chart Placeholder
                  </div>
                </div>
              </ChartWrapper>
            </div>

            {/* Split Charts */}
            <div className="col-lg-6 mb-4">
              <ChartWrapper title="역별 혼잡도 순위 (TOP N)" description="유동인구가 가장 많은 역사 순위 데이터">
                <div className="d-flex align-items-center justify-content-center h-100 text-muted border border-dashed rounded bg-light" style={{ minHeight: '300px' }}>
                  <p>Bar Chart Placeholder</p>
                </div>
              </ChartWrapper>
            </div>

            {/* ✅ 수정된 부분: 아래 ChartWrapper의 닫는 태그 순서를 바로잡았습니다. */}
            <div className="col-lg-6 mb-4">
              <ChartWrapper title="시간대별 승하차 패턴 비교" description="05시부터 24시까지의 평균 유동인구 분포">
                <div className="d-flex align-items-center justify-content-center h-100 text-muted border border-dashed rounded bg-light" style={{ minHeight: '300px' }}>
                  <p>Area/Radar Chart Placeholder</p>
                </div>
              </ChartWrapper> {/* 여기에 </ChartWrapper>가 위치해야 합니다. */}
            </div>
          </section>

        </div>
      </main>
    </div>
  );
};

// [6] Chart Wrapper Component
const ChartWrapper = ({ title, description, children }) => (
  <div className="card border-0 shadow-sm h-100">
    <div className="card-header bg-white py-3 border-0 d-flex justify-content-between align-items-center">
      <div>
        <h5 className="mb-1 fw-bold text-dark">{title}</h5>
        <p className="card-text small text-muted mb-0">{description}</p>
      </div>
      <button className="btn btn-link text-secondary p-0" data-bs-toggle="tooltip" title="상세 설명 보기">
        <i className="bi bi-info-circle"></i>
      </button>
    </div>
    <div className="card-body pt-0">
      {children}
    </div>
  </div>
);

export default MetroDashboard;