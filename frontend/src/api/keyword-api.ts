import { KeywordCount } from "../datatype/KeywordCount";
import { SearchResult, VideoItem } from "../datatype/SearchData";

const API_HOST = process.env.REACT_APP_ENV === "prod" ? process.env.REACT_APP_API : "";

export const checkSpelling = async (query: string) => {
  const url = `${API_HOST}/api/spellcheck?q=${query}`;
  const resp = await fetch(url);
  const spellcheck = await resp.text();
  return spellcheck;
}

export const getKeywordCount = async () => {
  const url = `${API_HOST}/api/trending`;
  const resp = await fetch(url);
  const count = await resp.json();
  return count as KeywordCount[];
}