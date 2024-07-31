/** @type {import('tailwindcss').Config} */
export default {
	content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
	theme: {
		screens: {
			"laptop": "1024px",
			"monitor": "1366px",
			"bigmonitor": "1809px"

		},
		backgroundImage:{
			'header': "url('/static/header/header.jpg')",
			'stats': "url('/static/other/statistics.jpg')",
			'reviews': "url('/static/other/reviews.jpg')"
		},
		extend: {
			colors: {
				'gold-color': '#eee150'
			}
		},
	},
	plugins: [],
}