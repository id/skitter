import { Toaster } from "@/components/ui/sonner";
import { DashboardShell } from "@/dashboard/components/dashboard-shell";
import { useSkitterDashboard } from "@/dashboard/use-skitter-dashboard";

function App() {
  const dashboard = useSkitterDashboard();

  return (
    <>
      <DashboardShell dashboard={dashboard} />
      <Toaster position="bottom-right" />
    </>
  );
}

export default App;
