import { Geist_Mono, Inter, JetBrains_Mono } from "next/font/google";
import { Footer, Layout, Navbar } from "nextra-theme-docs";
import { Head, Search } from "nextra/components";
import { getPageMap } from "nextra/page-map";
import "nextra-theme-docs/style.css";
import TitleFixer from "../components/TitleFixer";
import "./globals.css";

export const geistMono = Geist_Mono({
	variable: "--font-geist-mono",
	subsets: ["latin"],
	display: "swap",
});

export const inter = Inter({
	subsets: ["latin"],
	display: "swap",
});

export const jetbrains_mono = JetBrains_Mono({
	subsets: ["latin"],
	display: "swap",
});

export const metadata = {
	// Define your metadata here
	// For more information on metadata API, see: https://nextjs.org/docs/app/building-your-application/optimizing/metadata
};

const navbar = (
	<Navbar
		logo={<b>Strato</b>}
		// ... Your additional navbar options
	/>
);
const footer = (
	<Footer>
		<span>
			© {new Date().getFullYear()}{" "}
			<a href="https://iqua.ece.toronto.edu">
				iQua Group &middot; University of Toronto
			</a>
			. All rights reserved.
		</span>
	</Footer>
);
const search = <Search placeholder='Search ("/" to focus)' />;

export default async function RootLayout({ children }) {
	return (
		<html
			// Not required, but good for SEO
			lang="en"
			// Required to be set
			dir="ltr"
			// Suggested by `next-themes` package https://github.com/pacocoursey/next-themes#with-app
			suppressHydrationWarning
		>
			<Head
				color={{
					hue: { dark: 181, light: 201 },
					saturation: { dark: 86, light: 90 },
				}}
			>
				<title>
					Strato Documentation &middot; iQua Group &middot; University of
					Toronto
				</title>
				<meta
					name="description"
					content="Strato Documentation &middot; iQua Group &middot; University of Toronto"
				/>
			</Head>
			<body>
				<Layout
					navbar={navbar}
					pageMap={await getPageMap()}
					docsRepositoryBase="https://github.com/iqua/strato-docs"
					footer={footer}
					editLink={null}
					feedback={{ content: null }}
					search={search}
				>
					<TitleFixer />
					{children}
				</Layout>
			</body>
		</html>
	);
}
