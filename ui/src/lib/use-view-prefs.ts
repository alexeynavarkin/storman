import { useCallback, useEffect, useState } from "react";

const SHOW_HIDDEN_KEY = "storman.view.showHidden";

function readShowHidden(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return window.localStorage.getItem(SHOW_HIDDEN_KEY) === "1";
  } catch {
    return false;
  }
}

export function useViewPrefs() {
  const [showHidden, setShowHiddenState] = useState<boolean>(readShowHidden);

  useEffect(() => {
    function onStorage(e: StorageEvent) {
      if (e.key === SHOW_HIDDEN_KEY) setShowHiddenState(readShowHidden());
    }
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  const setShowHidden = useCallback((value: boolean) => {
    setShowHiddenState(value);
    try {
      window.localStorage.setItem(SHOW_HIDDEN_KEY, value ? "1" : "0");
    } catch {
      // ignore quota / privacy mode failures
    }
  }, []);

  return { showHidden, setShowHidden };
}
