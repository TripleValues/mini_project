import { createContext, useContext, useState, useEffect } from "react"
import { useNavigate } from "react-router";
import { api } from '@utils/network.js'

export const AuthContext = createContext()

const AuthProvider = ({children}) => {
  const [isLogin, setIsLogin] = useState(false)
  const navigate = useNavigate()

  const setAuth = status => {
    localStorage.setItem("user", status)
    setIsLogin(status)
    navigate("/")
  }

  const clearAuth = () => {
    localStorage.removeItem("user")
    setIsLogin(false)
    navigate("/")
  }

  const removeAuth = () => {
    clearAuth()
  }

  const checkAuth = () => {
    return localStorage.getItem("user") ? true : false
  }

  useEffect(() => {
    if(localStorage.getItem("user")) setIsLogin(true)
  }, [])

  return (
    <AuthContext.Provider value={{ isLogin, setAuth, removeAuth, clearAuth, checkAuth }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => useContext(AuthContext)

export default AuthProvider