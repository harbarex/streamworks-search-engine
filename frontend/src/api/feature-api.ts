import { Feature } from "../datatype/Feature";
import { SearchResult, VideoItem } from "../datatype/SearchData";

const API_HOST = process.env.REACT_APP_ENV === "prod" ? process.env.REACT_APP_API : "";

export const getFeatures = async () => {
  const url = `${API_HOST}/api/features`;
  const resp = await fetch(url, {
    method: "GET",
    headers: new Headers({
      'Authorization': 'Bearer ' + localStorage.getItem("accessToken")
    })
  });
  const features = await resp.json();
  return features as Feature[];
}

export const addFeature = async (feat: Feature) => {
  const url = `${API_HOST}/api/features`;
  await fetch(url, {
    method: "POST",
    headers: new Headers({
      'Authorization': 'Bearer ' + localStorage.getItem("accessToken"),
      'Content-Type': 'application/json',
    }),
    body: JSON.stringify(feat),
  });
  return true;
}

export const editFeature = async (feat: Feature) => {
  const url = `${API_HOST}/api/features`;
  await fetch(url, {
    method: "PUT",
    headers: new Headers({
      'Authorization': 'Bearer ' + localStorage.getItem("accessToken"),
      'Content-Type': 'application/json',
    }),
    body: JSON.stringify(feat),
  });
  return true;
}
