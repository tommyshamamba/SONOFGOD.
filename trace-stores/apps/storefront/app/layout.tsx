import "./styles.css";
import type { Metadata } from "next";
export const metadata: Metadata = { title: "Trace & Store", description: "Turn artwork into products in minutes." };
export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) { return <html lang="en"><body>{children}</body></html>; }
