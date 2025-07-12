# Vind

> **Vind** means "wind" in Scandinavian languages — light, fast, and always moving forward.

**Vind** is a lightweight, fast, and reliable web-based database management system (DBMS).  
Built with **Go** for the backend and **React** for the frontend, Vind aims to provide a minimalist yet powerful interface for managing SQL databases like PostgreSQL, MySQL, and SQLite.

---

## 🚀 Features

- Connect and query multiple databases (PostgreSQL, MySQL, SQLite, etc.)
- Simple SQL editor and result viewer
- Lightweight, fast, and secure by design
- Built with:
  - ⚙️ Golang (Gin) backend
  - ⚛️ React frontend

---

## 📁 Project Structure

```plaintext
vind/
├── backend/               # Golang backend (API, DB layer)
├── frontend/              # React frontend (UI)
├── .env                   # Root-level config (if shared)
├── docker-compose.yml     # Optional: full-stack setup
└── README.md              # You're here!
```

---

## 🛠️ Getting Started

### 1. Clone the Repo

```bash
git clone https://github.com/yourusername/vind.git
cd vind
```

### 2. Backend Setup (Go)

```bash
cd backend
go mod tidy
cp .env.example .env   # Add your DB credentials
go run cmd/main.go
```

### 3. Frontend Setup (React)

```bash
cd frontend
npm install
npm run dev
```

> Default frontend runs on http://localhost:5173

---

## ⚙️ Environment Variables

### `.env` (backend)

```dotenv
PORT=56789
```

---

## 🧪 Testing

**Backend (Go):**

```bash
go test ./...
```

**Frontend (React):**

```bash
npm run test
```

---

## 🐳 Docker Support (Optional)

```bash
docker-compose up --build
```

> Docker config is coming soon.

---

## 📌 Roadmap
<!-- 
- [ ] UI-based SQL editor
- [ ] Table schema editor
- [ ] Multi-database connections
- [ ] Query history and saved queries
- [ ] User auth and sessions -->

---

## 🤝 Contributing

Contributions are welcome! Just open an issue or submit a PR.

---

## 📄 License

MIT License © Abdul Hamid
