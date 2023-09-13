import { Button, Checkbox, FormControlLabel, Grid, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, Typography } from "@mui/material";
import React, { useEffect, useState } from "react";
import { addFeature, editFeature, getFeatures } from "../api/feature-api";
import { Feature } from "../datatype/Feature";

const ScoreCalculation = () => {
  const [features, setFeatures] = useState<Feature[]>([]);
  const [featName, setFeatName] = useState("");
  const [featCoeff, setFeatCoeff] = useState(0);
  const [featUseLog, setUseLog] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      const feats = await getFeatures();
      setFeatures(feats);
    };
    fetchData().catch(console.error);
  }, []);

  const getFormula = () => {
    const featFormula = features.map((feat) => `${feat.useLog ? "log(" + feat.name + ")" : feat.name}` + " * " + feat.coeff);
    return `Final Score = ${featFormula.join(" + ")}`;
  }

  const onSubmitFeat = async () => {
    const isEditting = features.filter((feat) => feat.name === featName).length > 0;
    const formFeat = { name: featName, coeff: featCoeff, useLog: featUseLog };
    if (isEditting) {
      await editFeature(formFeat);
      setFeatures(features.map((feat) => feat.name === featName ? formFeat : feat));
    } else {
      await addFeature(formFeat);
      setFeatures([...features, formFeat]);
    }
    setFeatName("");
    setFeatCoeff(0);
    setUseLog(false);
  }

  return (
    <Grid container spacing={1}>
      <Grid item xs={12}>
        <Paper elevation={3} style={{padding: "1px 10px 5px 10px", background: "#d3d3d3"}}>
          <Typography variant="h5" component="h3" align="center">
            {getFormula()}
          </Typography>
        </Paper>
      </Grid>
      <Grid item xs={12}>
        <h3>All Features</h3>
        <TableContainer>
          <Table sx={{ minWidth: 650 }} aria-label="simple table">
            <TableHead>
              <TableRow>
                <TableCell><b>Name</b></TableCell>
                <TableCell><b>Coefficient</b></TableCell>
                <TableCell><b>Use Log</b></TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {features.map((feat) => (
                <TableRow
                  key={feat.name}
                  sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                >
                  <TableCell component="th" scope="row">
                    {feat.name}
                  </TableCell>
                  <TableCell>{feat.coeff}</TableCell>
                  <TableCell>{feat.useLog ? "true" : "false"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Grid>
      <Grid item xs={12}>
        <hr />
        <h3>Add/Edit Features</h3>
        <b>Name</b>
        <TextField value={featName} onChange={(e) => setFeatName(e.target.value)} fullWidth size="small"></TextField>
      </Grid>
      <Grid item xs={12}>
        <b>Coefficient</b>
        <TextField value={featCoeff} onChange={(e) => setFeatCoeff(+e.target.value)} type="number" fullWidth size="small"></TextField>
      </Grid>
      <Grid item xs={12}>
        <FormControlLabel label="Use Log" control={
          <Checkbox
            checked={featUseLog}
            onChange={(e) => setUseLog(e.target.checked)}
            inputProps={{ 'aria-label': 'controlled' }}
          />}
        />
      </Grid>
      <Grid item xs={2}>
        <Button variant="contained" onClick={onSubmitFeat} fullWidth>Submit</Button>
      </Grid>
    </Grid>
  )
}

export default ScoreCalculation;