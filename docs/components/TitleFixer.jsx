"use client";

import { useEffect } from "react";

export default function TitleFixer() {
	const targetTitle = "Nextmini Documentation";

	useEffect(() => {
		// Function to set the title
		const setTitle = () => {
			if (document.title !== targetTitle) {
				document.title = targetTitle;
			}
		};

		// Set the title immediately
		setTitle();

		// Set the title again after 500ms to catch Nextra overrides
		const timeout = setTimeout(setTitle, 500);

		// Cleanup timeout when the component unmounts or pathname changes
		return () => {
			clearTimeout(timeout);
		};
	}, []); // Re-run effect on route changes

	return null; // This component renders nothing
}
