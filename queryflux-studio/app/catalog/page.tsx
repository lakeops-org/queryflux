import { getCatalogProviderConfig } from "@/lib/api";
import { CatalogEditor } from "./catalog-editor";

export const revalidate = 0;

export default async function CatalogPage() {
  const config = await getCatalogProviderConfig().catch(() => null);

  return <CatalogEditor initialConfig={config} />;
}
