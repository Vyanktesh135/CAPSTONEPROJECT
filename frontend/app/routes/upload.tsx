import { redirect, Form, Link } from "react-router";
import type { Route } from "./+types/home";
import {
  Field,
  FieldGroup,
  FieldLegend,
  FieldLabel,
} from "~/components/ui/field";
import { Input } from "~/components/ui/input";
import { Button } from "~/components/ui/button";

export async function clientAction({ request }: Route.ClientActionArgs) {
  const form_data = await request.formData();

  const response = await fetch("/api/upload", {
    method: "POST",
    body: form_data,
  });
 
  const data = await response.json()
  if (!response.ok) {
    // optionally return something for UI
    alert(data.error)
    return redirect("/")
  }
 
  console.log(data);
  return redirect(`/?file_name=${data.file_name}&table_name=${data.table_name}`);
}

export default function Upload_form() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-zinc-900 px-4 py-10">
      <Form method="post" encType="multipart/form-data" className="w-full max-w-lg">
        <FieldGroup className="rounded-2xl border border-zinc-700/50 bg-zinc-800 shadow-xl">
          {/* Header */}
          <div className="border-b border-zinc-700/50 px-8 py-6">
            <FieldLegend className="text-lg font-semibold text-zinc-100">
              Upload Sales CSV
            </FieldLegend>
            <p className="mt-1 text-sm text-zinc-400">
              Import sales data from a CSV file.
            </p>
          </div>

          {/* Body */}
          <div className="px-8 py-6 space-y-3">
            <FieldLabel htmlFor="file" className="text-sm font-medium text-zinc-200">
              CSV File
            </FieldLabel>

            <Field>
              <Input
                id="file"
                name="file"
                type="file"
                required
                className="h-11 bg-zinc-900 border border-zinc-700 text-zinc-100
                           file:mr-4 file:rounded-md file:border-0 file:bg-zinc-700
                           file:px-4 file:py-2 file:text-sm file:font-medium file:text-zinc-100
                           hover:file:bg-zinc-600
                           focus-visible:ring-2 focus-visible:ring-indigo-400
                           focus-visible:ring-offset-2 focus-visible:ring-offset-zinc-800"
              />
            </Field>

            <p className="text-xs text-zinc-400">
              Supported format: CSV only
            </p>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 border-t border-zinc-700/50 px-8 py-5">
            <Button
              type="button"
              variant="outline"
              asChild
              className="h-10 px-5 border-zinc-600 text-zinc-200 hover:bg-zinc-700"
            >
              <Link to="/"
              className="h-10 px-5 border-zinc-600 text-zinc-200 hover:bg-zinc-700">Cancel</Link>
            </Button>

            <Button
              type="submit"
              className="h-10 px-5 bg-indigo-500 text-white hover:bg-indigo-400"
            >
              Upload
            </Button>
          </div>
        </FieldGroup>
      </Form>
    </div>
  );
}

// export default function Upload_form() {
//   return (
//     <div className="min-h-screen flex items-center justify-center bg-gradient-to-b from-muted/20 to-background px-4 py-10">
//       <Form method="post" encType="multipart/form-data" className="w-full max-w-md">
//         <FieldGroup className="rounded-2xl border bg-background p-6 shadow-lg ring-1 ring-black/5">
//           <FieldLegend className="text-base font-semibold tracking-tight">
//             Upload Sales CSV
//           </FieldLegend>
//           <p className="mt-1 text-sm text-muted-foreground">
//             Upload a file to process and store your sales records.
//           </p>

//           <div className="mt-6 space-y-2">
//             <FieldLabel htmlFor="file" className="text-sm font-medium">
//               CSV file
//             </FieldLabel>
//             <Field>
//               <Input
//                 id="file"
//                 name="file"
//                 type="file"
//                 required
//                 className="h-11 file:mr-3 file:rounded-md file:border file:border-input file:bg-background file:px-3 file:py-2 file:text-sm file:font-medium focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
//               />
//             </Field>
//           </div>

//           <div className="mt-7 flex gap-3">
//             <Button type="button" variant="outline" asChild className="h-10 flex-1">
//               <Link to="/">Cancel</Link>
//             </Button>
//             <Button type="submit" className="h-10 flex-1">
//               Upload
//             </Button>
//           </div>
//         </FieldGroup>
//       </Form>
//     </div>
//   );
// }

// export default function Upload_form() {
//   return (
//     <div className="min-h-screen bg-muted/30 flex items-center justify-center px-4 py-10">
//       <Form method="post" encType="multipart/form-data" className="w-full max-w-lg">
//         <FieldGroup className="rounded-2xl border bg-background shadow-md">
//           <div className="border-b px-8 py-6">
//             <FieldLegend className="text-lg font-semibold tracking-tight">
//               Upload Sales CSV
//             </FieldLegend>
//             <p className="mt-1 text-sm text-muted-foreground">
//               Choose a CSV file to import sales data.
//             </p>
//           </div>

//           <div className="px-8 py-6 space-y-3">
//             <FieldLabel htmlFor="file" className="text-sm font-medium">
//               File
//             </FieldLabel>

//             <Field>
//               <Input
//                 id="file"
//                 name="file"
//                 type="file"
//                 required
//                 className="h-11 file:mr-4 file:rounded-md file:border-0 file:bg-muted file:px-4 file:py-2 file:text-sm file:font-medium hover:file:bg-muted/80"
//               />
//             </Field>

//             <p className="text-xs text-muted-foreground">
//               Accepted format: .csv
//             </p>
//           </div>

//           <div className="flex items-center justify-end gap-3 border-t px-8 py-5">
//             <Button type="button" variant="outline" asChild className="h-10 px-5">
//               <Link to="/">Cancel</Link>
//             </Button>
//             <Button type="submit" className="h-10 px-5">
//               Upload
//             </Button>
//           </div>
//         </FieldGroup>
//       </Form>
//     </div>
//   );
// }
