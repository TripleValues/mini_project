import '@styles/LoadingOverlay.css';

const LoadingOverlay = ({ message = '데이터 로딩 중...' }) => {
  return (
    <div className="overlay">
      <div className="spinner" />
      <p className="msg">{message}</p>
    </div>
  );
}

export default LoadingOverlay;