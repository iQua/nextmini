export default {
	head: (
		<>
			<title>Performance Software Systems with Rust</title>
			<meta
				property="og:title"
				content="Performant Software Systems with Rust"
			/>
			<meta
				property="og:description"
				content="Performant Software Systems with Rust"
			/>
		</>
	),
	logo: <b>Performant Software Systems with Rust</b>,
	docsRepositoryBase: "https://github.com/baochunli/ece1724-f25",
	project: {
		link: "https://github.com/baochunli/ece1724-f25",
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
