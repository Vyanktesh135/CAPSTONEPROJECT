import { useState,useEffect} from "react"
import { Button } from "~/components/ui/button"
import { Link } from "react-router"
import { useSearchParams ,useFetcher} from "react-router"
import type { Route } from "./+types/home"

export async function clientLoader(){
  const response = await fetch('/api/getfiles',{
    method: 'GET'
  })
  console.log(response)
  const json_data = await response.json()
  console.log("Json Data ",json_data)
  const temp_test = [
    {
      "file_name": "fact_sales.csv",
      "table_name": "sales_75885eaab491"
    },
    {
      "file_name": "synthatic_data.csv",
      "table_name": "sales_91f186fe17ce"
    },
    {
      "file_name": "sales_data_sample.csv",
      "table_name": "sales_e0ceb4a5d914"
    },
    {
      "file_name": "customer_data.csv",
      "table_name": "customer_1a2b3c4d5e6f"
    },
    {
      "file_name": "product_catalog.csv",
      "table_name": "product_7g8h9i0j1k2l"
    },
    {
      "file_name": "inventory.csv",
      "table_name": "inventory_3m4n5o6p7q8r"
    },
    {
      "file_name": "orders.csv",
      "table_name": "orders_9s0t1u2v3w4x"
    },
    {
      "file_name": "payments.csv",
      "table_name": "payments_5y6z7a8b9c0d"
    },
    {
      "file_name": "shipping_data.csv",
      "table_name": "shipping_1e2f3g4h5i6j"
    },
    {
      "file_name": "returns.csv",
      "table_name": "returns_7k8l9m0n1o2p"
    },
    {
      "file_name": "marketing_campaigns.csv",
      "table_name": "marketing_3q4r5s6t7u8v"
    },
    {
      "file_name": "website_traffic.csv",
      "table_name": "traffic_9w0x1y2z3a4b"
    },
    {
      "file_name": "email_subscribers.csv",
      "table_name": "subscribers_5c6d7e8f9g0h"
    },
    {
      "file_name": "feedback.csv",
      "table_name": "feedback_1i2j3k4l5m6n"
    },
    {
      "file_name": "sales_forecast.csv",
      "table_name": "forecast_7o8p9q0r1s2t"
    },
    {
      "file_name": "finance_summary.csv",
      "table_name": "finance_3u4v5w6x7y8z"
    },
    {
      "file_name": "employee_data.csv",
      "table_name": "employee_9a0b1c2d3e4f"
    },
    {
      "file_name": "supplier_data.csv",
      "table_name": "supplier_5g6h7i8j9k0l"
    },
    {
      "file_name": "logistics.csv",
      "table_name": "logistics_1m2n3o4p5q6r"
    },
    {
      "file_name": "budget.csv",
      "table_name": "budget_7s8t9u0v1w2x"
    },
    {
      "file_name": "audit_logs.csv",
      "table_name": "audit_3y4z5a6b7c8d"
    },
    {
      "file_name": "project_plan.csv",
      "table_name": "project_9e0f1g2h3i4j"
    },
    {
      "file_name": "meeting_notes.csv",
      "table_name": "meetings_5k6l7m8n9o0p"
    }
  ]
  return temp_test
}

export async function clientAction({request}:Route.ClientActionArgs){
  const form_data = await request.formData()
  const response = await fetch("/api/analyse",{
    method: 'POST',
    body: form_data
    }
  )
  const data = response.json()
}
export default function Home({ loaderData }: any) {
  const [file_status, setFile_status] = useState(false);
  const [file_att,setFile_name] = useState({"file_name":"","table_name":""})
  const [searchParams] = useSearchParams();
  const fetcher = useFetcher();
  const [query,setQuery] = useState("Type Some Message Here ..");

  useEffect(() => {
    const file_name = searchParams.get("file_name");
    const table_name = searchParams.get("table_name");

    if (file_name && table_name) {
      setFile_name({ "file_name":file_name,"table_name": table_name });
      setFile_status(true);
    }
  }, [searchParams]);

  const triggerAction = () => {
    const data = new FormData();
    data.append("query", query);
    data.append("table_name",file_att.table_name)

    fetcher.submit(data, {
      method: "post",
      action: "/", // 
    });
    setQuery("")
  };
  console.log(file_att)
  console.log(file_status)
  const select_file = async (table_name:any) => {
    setFile_status(true)
    setFile_name(table_name)
    console.log(file_att)
  }
  return (
    <div className="min-h-screen w-full bg-zinc-900 text-zinc-100">
      <div className="mx-auto flex h-[calc(100vh-4rem)] max-w-7xl gap-6 px-4 py-8 min-h-0">
        {file_status ? (
          <div className="flex w-[70%] flex-col rounded-2xl border border-zinc-700/50 bg-zinc-800 shadow-xl min-h-0">
            {/* Header */}
            <div className="shrink-0 border-b border-zinc-700/50 px-6 py-5">
              <h1 className="text-lg font-semibold">Chat</h1>
              <p className="mt-1 text-sm text-zinc-400">
                <b>Ask questions about your <u><i>{file_att.file_name}</i></u> files.</b>
              </p>
            </div>

            {/* Messages */}
            <div className="flex-1 min-h-0 px-6 py-5">
              <div className="h-full rounded-xl border border-dashed border-zinc-700/60 bg-zinc-900/30 p-4 text-sm text-zinc-400">
                Messages will appear here.
              </div>
            </div>

            {/* Input */}
            <div className="shrink-0 border-t border-zinc-700/50 px-6 py-5">
              <div className="flex gap-3">
                <input
                  className="h-11 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-4 text-sm text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2 focus:ring-offset-zinc-800"
                  placeholder={`${query}`}
                  id = "message"
                  onChange={(e)=>setQuery(e.target.value)}
                />
                <button className="h-11 rounded-lg bg-indigo-500 px-6 text-sm font-medium text-white hover:bg-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2 focus:ring-offset-zinc-800"
                onClick={triggerAction}>
                  Send
                </button>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex w-[70%] flex-col items-center justify-center rounded-2xl border border-zinc-700/50 bg-zinc-800 shadow-xl">
            <div className="mx-auto w-full max-w-sm rounded-2xl border border-zinc-700/50 bg-zinc-900/30 p-6 text-center">
              <h2 className="text-base font-semibold text-zinc-100">
                Start by choosing a file
              </h2>
              <p className="mt-1 text-sm text-zinc-400">
                Upload a new CSV to begin chatting.
              </p>

              <div className="mt-5">
                <Button className="h-11 w-full bg-indigo-500 text-white hover:bg-indigo-400" asChild>
                  <Link
                    to="/upload_file"
                    className="w-full"
                  >
                    Upload new file
                  </Link>
                </Button>
              </div>
            </div>
          </div>
        )}

        {/* RIGHT: Files Panel (30%) */}
        <div className="w-[30%] flex flex-col rounded-2xl border border-zinc-700/50 bg-zinc-800 shadow-xl min-h-0">
          {/* Header (fixed) */}
          <div className="shrink-0 border-b border-zinc-700/50 px-6 py-5">
            <h2 className="text-lg font-semibold">Uploaded Files</h2>
            <p className="mt-1 text-sm text-zinc-400">
              Click a file to view or chat about it.
            </p>
          </div>

          {/* Scrollable File List */}
          <div className="flex-1 min-h-0 overflow-y-auto px-6 py-5">
            <div className="space-y-3">
              {loaderData.map((data: any, idx: any) => (
                <div
                  key={data.file_name ?? idx}
                  className="group flex items-center justify-between rounded-xl border border-zinc-700/60 bg-zinc-900/30 px-4 py-3 hover:bg-zinc-900/50 hover:border-zinc-600 transition"
                >
                  <div className="min-w-0">
                    <h3 className="truncate text-sm font-medium text-zinc-100">
                      {data.file_name}
                    </h3>
                    <p className="mt-0.5 text-xs text-zinc-500">
                      Ready to query
                    </p>
                  </div>

                  <div className="text-xs font-medium text-indigo-300 opacity-0 group-hover:opacity-100 transition">
                    <Button onClick={() => select_file({"table_name":data.table_name,"file_name":data.file_name})}>Open</Button>
                  </div>
                </div>
              ))}

              {(!loaderData || loaderData.length === 0) && (
                <div className="rounded-xl border border-dashed border-zinc-700/60 bg-zinc-900/30 p-6 text-sm text-zinc-400">
                  No files found yet.
                </div>
              )}
            </div>
          </div>
          { file_status?
          <Button
          asChild
          className="h-11 px-6 rounded-lg bg-indigo-500 text-white
                    hover:bg-indigo-400 focus-visible:ring-2
                    focus-visible:ring-indigo-400 focus-visible:ring-offset-2
                    focus-visible:ring-offset-zinc-900"
        >
          <Link to="/upload_file">UPLOAD & QUERY</Link>
        </Button> : null}
        </div>
      </div>
    </div>
  );
}

