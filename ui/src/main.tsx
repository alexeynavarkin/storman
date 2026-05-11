import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";

import { Providers } from "@/app/providers";
import { RootLayout } from "@/app/root-layout";
import "@/styles/globals.css";

const root = document.getElementById("root");
if (!root) throw new Error("root element missing");

createRoot(root).render(
  <StrictMode>
    <BrowserRouter>
      <Providers>
        <RootLayout />
      </Providers>
    </BrowserRouter>
  </StrictMode>,
);
