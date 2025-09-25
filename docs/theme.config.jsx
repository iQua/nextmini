export default {
	head: (
		<>
			<title>Nextmini Documentation</title>
			<meta property="og:title" content="Nextmini Documentation" />
			<meta property="og:description" content="Nextmini Documentation" />
		</>
	),
	logo: <b>Nextmini Documentation</b>,
	docsRepositoryBase: "https://github.com/iqua/nextmini",
	project: {
		link: "https://github.com/iqua/nextmini",
	},
	footer: {
		content: (
			<span>
				© {new Date().getFullYear()}{" "}
				<a
					href="https://iqua.ece.toronto.edu/bli"
					target="_blank"
					rel="noopener"
				>
					Baochun Li
				</a>
				. All rights reserved.
			</span>
		),
	},
	editLink: {
		component: null,
	},
	feedback: {
		content: null,
	},
	color: {
		hue: { dark: 181, light: 201 },
		saturation: { dark: 86, light: 90 },
	},
	search: {
		placeholder: 'Search ("/" to focus)',
	},
};
