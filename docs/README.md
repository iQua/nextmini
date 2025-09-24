# Nextmini Documentation

This directory contains the latest documentation for the Nextmini project.

# Building the website

To install Nextra and Next.js after installing [Bun](https://bun.sh/docs/installation), run:

```shell
bun add next react react-dom nextra nextra-theme-docs steps geist sharp
```

or simply:

```shell
bun install
```

To start the development server, run:

```shell
bun run dev
```

Then point the browser to `http://localhost:3000/~bli/eecg` to view the website, where `~bli/eecg` is the `basepath` of the website specified in `next.config.js`.

In case `pagefind` has not yet been installed, it can be installed with:

```shell
cargo install pagefind --features extended
```

To build the static website, run:

```shell
bun run build
```

To serve the static website, run:

```shell
bun run start
```

or:

```shell
npx serve@latest out
```

Before deploying the static website, configure `basepath` in `next.config.js` to match the deployment URL. For example, if the website is deployed at `https://www.eecg.toronto.edu/~bli/eecg`, the `basepath` should be set to `/~bli/eecg`.
