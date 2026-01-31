import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 50,         // 50 parallel users
  duration: "30s", // run for 30 seconds (change as needed)
};

export default function () {
  const url = "http://curepoint.in/syncProductsToElixireMobile/?isFirstTimeSync=true";

  const params = {
    headers: {
      "application": "ELIXIRE_MOBILE",
      "storeID": "ELXS218598",
      "userID": "ELXU114905708",
      "posID": "ELIXIRE_MOBILE"
    }
  };

  const res = http.get(url, params);

  console.log(`Status: ${res.status} | Duration: ${res.timings.duration} ms`);

  check(res, {
    "status is 200": (r) => r.status === 200,
  });
}
