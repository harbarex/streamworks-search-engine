import { Grid } from "@mui/material";
import Pagination from "@mui/material/Pagination";
import React from "react";

const PaginationSection = ({ count, onPageChange, pageSize }: {
  count: number;
  onPageChange: (value: number) => void;
  pageSize: number
}) => {
  if (count <= 1) return null;
  return (
    <Grid item xs={12}>
      <Pagination 
        count={count} 
        shape="rounded" 
        onChange={(e, page) => onPageChange(page)} size="large" />
      {/* <Grid container justifyItems="center" alignContent="center">
        <Grid item xs={3}></Grid>
        <Grid item xs={6}>
          
        </Grid>
      </Grid> */}
    </Grid>
  )
}

export default PaginationSection;
