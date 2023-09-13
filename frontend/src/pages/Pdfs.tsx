import { Button, ButtonGroup, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TablePagination, TableRow } from "@mui/material";
import Grid from "@mui/material/Grid";
import React, { useEffect, useState } from "react";
import { search } from "../api/search-api";
import SearchSection from "../components/SearchSection";
import { SearchResult } from "../datatype/SearchData";
import { useQueryParam } from "../hooks/useQueryParam";
import NorthEastIcon from '@mui/icons-material/NorthEast';

const Pdfs = () => {
  const { query, onSearchQuery } = useQueryParam();
  const [searchTerm, setSearchTerm] = useState<string>(query.get("q") || "");
  const [pdfs, setPdfs] = useState<SearchResult[]>([]);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const triggerSearch = () => {
    onSearchQuery(searchTerm);
  }

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(+event.target.value);
    setPage(0);
  };

  useEffect(() => {
    const fetchData = async () => {
      if (query.get("q") !== null) {
        const results = await search(query.get("q") || "", "pdf");
        setPdfs(results);
      }
    };
    fetchData().catch(console.error);
  }, [query]);

  return (
    <Grid container style={{padding: '15px'}} spacing={1}>
      <Grid item xs={12}>
        <SearchSection query={searchTerm} onQueryChange={setSearchTerm} onQueryEntered={triggerSearch} showDebugOption={false} />
        <hr />
      </Grid>
      <Grid item xs={12}>
        <Paper sx={{ width: '100%' }}>
          <TableContainer>
            <Table stickyHeader aria-label="sticky table">
              <TableHead>
                <TableRow>
                  <TableCell align="center" colSpan={3}>
                    <b>PDF Title</b>
                  </TableCell>
                  <TableCell align="center" colSpan={1}>
                    <b>Action</b>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {pdfs
                  .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                  .map((pdf, index) => {
                    return (
                      <TableRow hover role="checkbox" tabIndex={-1} key={index}>
                        <TableCell colSpan={3} align="center">
                          {pdf.title}
                        </TableCell>
                        <TableCell align="center" colSpan={1}>
                          <ButtonGroup variant="contained">
                            <Button color="primary" onClick={() => window.open(pdf.url)}>
                              <NorthEastIcon/>
                            </Button>
                          </ButtonGroup>
                        </TableCell>
                      </TableRow>
                    );
                  })}
              </TableBody>
            </Table>
          </TableContainer>
          <TablePagination
            rowsPerPageOptions={[10]}
            component="div"
            count={pdfs.length}
            rowsPerPage={rowsPerPage}
            page={page}
            onPageChange={handleChangePage}
            onRowsPerPageChange={handleChangeRowsPerPage}
          />
        </Paper>
      </Grid>
    </Grid>
  )
}

export default Pdfs;
