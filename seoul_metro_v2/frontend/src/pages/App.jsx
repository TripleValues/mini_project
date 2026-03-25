import "bootstrap/dist/css/bootstrap.min.css";
import "bootstrap/dist/js/bootstrap.bundle.min.js";
import { BrowserRouter, Routes, Route } from "react-router";
import {api} from "@utils/network.js"

const Home = () => {
  const btn1Event = () => {
    console.log("btn1 호출")

    api.get("/hello")
      .then(res => {
        console.log(res.data)
        if(res.data.status) {
          alert(res.data.result[0])
        } else {
          alert("오류 발생")
        }
      })
      .catch(err => console.error(err))
      .finally(() => console.log("완료"))

  }
  return (
    <div className="text-center">
      <h1>메인 화면입니다.</h1>
      <button type="button" onClick={btn1Event}>FastAPI 확인</button>
    </div>
  )
}

const NotFound = () => {
  return (
    <div className="text-center">
      <h1>404</h1>
      <p>페이지를 찾을 수 없습니다.</p>
    </div>
  )
}

const App = () => {
  const paths = [
    {path: "/", element: <Home />},
    {path: "*", element: <NotFound />},
  ]
  return (
    <>
      <BrowserRouter>
        <Routes>
          { paths?.map((v, i) => <Route key={i} path={v.path} element={v.element} />) }
        </Routes>
      </BrowserRouter>
    </>
  )
}

export default App