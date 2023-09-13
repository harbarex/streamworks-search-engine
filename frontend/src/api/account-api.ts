import jwt_decode from "jwt-decode";
import { User } from "../datatype/User";

const API_HOST = process.env.REACT_APP_ENV === "prod" ? process.env.REACT_APP_API : "";

export const login = async (username: string, password: string) => {
  const url = `${API_HOST}/api/login?username=${username}&password=${password}`;
  const resp = await fetch(url);
  if (resp.ok) {
    const token = await resp.text();
    const user: User = jwt_decode(token) as User;
    localStorage.setItem("username", user.username);
    localStorage.setItem("isAdmin", `${user.isAdmin}`);
    localStorage.setItem("accessToken", token);
    return [true, user];
  }
  return [false, null];
}

export const register = async (username: string, password: string) => {
  const url = `${API_HOST}/api/register?username=${username}&password=${password}`;
  const resp = await fetch(url);
  if (resp.ok) {
    const token = await resp.text();
    const user: User = jwt_decode(token) as User;
    localStorage.setItem("username", user.username);
    localStorage.setItem("isAdmin", "false");
    localStorage.setItem("accessToken", token);
    return [true, user];
  }
  return [false, null];
}
