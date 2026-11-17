"use client";

import { useState } from "react";

export default function CheckoutPage() {
  const [status, setStatus] = useState("idle");

  const handleCheckout = async () => {
    setStatus("processing");
    try {
      // 1. Fetch a valid signed JWT from the API Gateway
      const tokenRes = await fetch("http://localhost:8080/api/auth/token", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: "user_web_123" }),
      });
      const { token } = await tokenRes.json();

      // 2. Dispatch checkout through API Gateway enforcing JWT authentication
      const response = await fetch("http://localhost:8080/api/payment", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`, 
        },
        body: JSON.stringify({
          cart: [{ id: 1, name: "Kafka Hoodie", price: 50 }],
        }),
      });

      if (!response.ok) {
        throw new Error("Payment declined by gateway");
      }

      setStatus("success");
    } catch (error) {
      console.error(error);
      setStatus("error");
    }
  };

  return (
    <main className="flex min-h-screen flex-col items-center justify-center p-24 bg-gray-50 text-black">
      <div className="max-w-md w-full bg-white p-8 rounded-xl shadow-lg border border-gray-200">
        <h1 className="text-3xl font-bold mb-6 text-gray-800">StreamMart Checkout</h1>
        
        <div className="bg-gray-100 p-4 rounded-lg mb-6 shadow-inner">
          <div className="flex justify-between items-center font-medium text-gray-700">
            <span>1x Kafka Hoodie</span>
            <span>$50.00</span>
          </div>
          <hr className="my-3 border-gray-300" />
          <div className="flex justify-between items-center font-bold text-gray-900">
            <span>Total</span>
            <span>$50.00</span>
          </div>
        </div>

        <button
          onClick={handleCheckout}
          disabled={status === "processing"}
          className={`w-full py-4 px-4 rounded-lg font-semibold text-white transition-all shadow-md ${
            status === "processing" ? "bg-gray-400 cursor-not-allowed" : "bg-blue-600 hover:bg-blue-700 hover:-translate-y-1"
          }`}
        >
          {status === "processing" ? "Processing via API Gateway..." : "Pay $50.00"}
        </button>

        {status === "success" && (
          <div className="mt-6 p-4 bg-green-100 text-green-800 rounded-lg border border-green-300 shadow-sm">
            <p className="font-bold text-lg mb-1">Payment Successful! 🎉</p>
            <p className="text-sm">Data saved to DB and asynchronously published to Kafka via the Outbox pattern.</p>
          </div>
        )}

        {status === "error" && (
          <div className="mt-6 p-4 bg-red-100 text-red-800 rounded-lg border border-red-300 shadow-sm">
            <p className="font-bold text-lg mb-1">Payment Failed ❌</p>
            <p className="text-sm">Check if the API Gateway (port 8080) and Neon PostgreSQL Database are running.</p>
          </div>
        )}
      </div>
    </main>
  );
}
