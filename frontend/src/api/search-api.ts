import { SearchResult, VideoItem } from "../datatype/SearchData";

const API_HOST = process.env.REACT_APP_ENV === "prod" ? process.env.REACT_APP_API : "";

export const search = async (term: string, type: string) => {
  const url = `${API_HOST}/api/search?q=${term}&type=${type}`;
  const resp = await fetch(url);
  const searchResults = await resp.json();
  return searchResults as SearchResult[];
}

export const searchYoutube = async (term: string) => {
  const API_KEY = process.env.REACT_APP_YOUTUBE_API_KEY;
  const url = `https://www.googleapis.com/youtube/v3/search?q=${term}&part=snippet&maxResults=90&key=${API_KEY}`;
  const resp = await fetch(url);
  const searchResult = (await resp.json())["items"];
  return searchResult as VideoItem[];
}
