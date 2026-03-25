import axios from "axios"

export const api = axios.create({
  baseURL: import.meta.env.VITE_SEOUL_METRO_URL || "http://localhost:8000" ,
  // withCredentials: true,     // 쿠키 안쓴다면 주석처리
  headers: {
    "Content-Type": "application/json",
  },
})
