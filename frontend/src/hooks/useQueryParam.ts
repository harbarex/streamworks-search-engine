import React from "react";
import { useLocation, useNavigate } from "react-router"

export const useQueryParam = () => {
  const { pathname, search } = useLocation();
  const navigate = useNavigate();

  const onSearchQuery = (term: string) => {
    navigate({
      pathname,
      search: term.length > 0 ? `?q=${term}` : ""
    }, { replace: true });
  }

  return {
    query: React.useMemo(() => new URLSearchParams(search), [search]), 
    onSearchQuery
  };
}