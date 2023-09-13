import { Alert, AlertTitle, Link, Paper, Skeleton } from "@mui/material";
import Grid from "@mui/material/Grid";
import React, { useEffect, useState } from "react";
import { search } from "../api/search-api";
import PaginationSection from "../components/PaginationSection";
import SearchSection from "../components/SearchSection";
import { SearchResult } from "../datatype/SearchData";
import { useQueryParam } from "../hooks/useQueryParam";

const Documents = () => {
  const { query, onSearchQuery } = useQueryParam();
  const [searchTerm, setSearchTerm] = useState<string>(query.get("q") || "");
  const [documents, setDocuments] = useState<SearchResult[]>([]);
  const [page, setPage] = useState(1);
  const isAdmin = localStorage.getItem("isAdmin") === "true";
  const [debugMode, setDebugMode] = useState(false);
  const [loading, setLoading] = useState(false);

  const triggerSearch = () => {
    onSearchQuery(searchTerm);
  }

  useEffect(() => {
    setLoading(true);
    const fetchData = async () => {
      if (query.get("q") !== null) {
        const results = await search(query.get("q") || "", "doc");
        setDocuments(results);
      }
      setLoading(false);
    };
    fetchData().catch(() => {
      setLoading(false);
    });
  }, [query]);

  const pageChangeHandler = (pageNumber: number) => {
    setPage(pageNumber);
  }


  return (
    <Grid container style={{padding: '15px'}} spacing={1}>
      <Grid item xs={12}>
        <SearchSection 
          query={searchTerm}
          onQueryChange={setSearchTerm}
          onQueryEntered={triggerSearch}
          showDebugOption={isAdmin}
          debugMode={debugMode}
          onDebugModeChange={(e) => setDebugMode(e.target.checked)}
        />
        <hr />
      </Grid>

      {
        loading && (
          <Grid item xs={12}>
            <Grid container spacing={1}>
              {
                [0,1,2,3,4].map((idx) => <Grid item xs={12}>
                  <Skeleton id={`${idx}`} variant="rectangular" width={"100%"} height={118} animation="wave" />
                </Grid>)
              }
            </Grid>
          </Grid>
        )
      }
      {
        !loading && query.get("q") && documents.slice((page - 1) * 10, page * 10).map((doc, index) => (
          <Grid item xs={12} key={index}>
            <Paper elevation={1} style={{padding: "1px 10px 5px 10px"}}>
              <h3><Link href={doc.url} style={{textDecoration: "none"}}>{doc.title || "(No title)"}</Link></h3>
              <div dangerouslySetInnerHTML={{__html: doc.displayText.trim() !== "..."  ? doc.displayText.trim() : doc.title}}></div>              
              {
                debugMode && (
                  <>
                    <hr/>
                    <b>Debug:</b>
                    { `Total Score: ${doc.totalScore} | Features: ${JSON.stringify(doc.features)}` }
                  </>
                )
              }
            </Paper>
          </Grid>
        ))
      }
      <Grid item xs={12}>
        {
          !loading && query.get("q") && documents.length === 0 && 
          <Alert severity="warning" style={{width: "100%"}}>
            <AlertTitle>Ooops</AlertTitle>
              We cannot find anything matching <b>{query.get("q")}</b>
          </Alert>
        }
        {
          !loading && query.get("q") == null && documents.length === 0 && 
          <Alert severity="info" style={{width: "100%"}}>
            <AlertTitle>Hey!</AlertTitle>
              Enter something you want to search in the box above
          </Alert>
        }
      </Grid>
      <PaginationSection count={Math.ceil(documents.length / 10)} onPageChange={pageChangeHandler} pageSize={10} />
    </Grid>
  )
}

export default Documents;
