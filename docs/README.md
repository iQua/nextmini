# Nextmini Documentation

This directory contains the latest documentation for the Nextmini project.

# Building the website

To install Nextra and Next.js after installing [Bun](https://bun.sh/docs/installation), run:

```shell
bun add next react react-dom nextra nextra-theme-docs steps geist sharp
```

To start the development server, run:

```shell
bun run dev
```

Then point the browser to `http://localhost:3000` to view the website.

To build the static website, run:

```shell
bun run build
```

To update all the dependencies, run:

```shell
bun update
```

To update `bun` itself, run:

```shell
bun upgrade
```

To serve the static website, run:

```shell
npx serve@latest out
```

Before deploying the static website, one may optionally configure `basepath` in `next.config.js` to match the deployment URL. For example, if the website is deployed at `https://www.eecg.toronto.edu/~bli/nextmini`, the `basepath` should be set to `/~bli/nextmini`.
