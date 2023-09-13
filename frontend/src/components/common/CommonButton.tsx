import React from "react";
import { Button, SxProps, Theme } from "@mui/material";
import { StringDecoder } from "string_decoder";

const CommonButton = ({ children, color, disabled, size, variant, sx }: {
  children?: string,
  color?: "inherit" | "primary" | "secondary" | "success" | "error" | "info" | "warning" | undefined,
  disabled?: boolean,
  size?: "small" | "medium" | "large" | undefined,
  variant?: "text" | "outlined" | "contained" | undefined,
  sx?: SxProps<Theme>,
}) => {
  return (
    <Button 
      color={color}
      disabled={disabled}
      size={size}
      variant={variant}
      sx={sx}
    >
      { children }
    </Button>
  )
}

export default CommonButton;
