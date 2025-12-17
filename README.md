**Problem Statement**

Merchandising analysts frequently need to analyze large sales datasets across multiple dimensions such as region, item type, sales channel, and time period.
Answering questions like “How did Beverages perform in Sub-Saharan Africa during Q2?” or “Which item types generated the highest online revenue last year?” requires complex filtering and aggregation, making manual analysis slow and inefficient.

This project aims to build a natural language–based analytics retrieval system that allows users to ask business questions in free text and receive accurate, aggregated, and explainable insights.
The system identifies relevant data subsets, computes key metrics (units sold, revenue, average selling price), and supports multi-faceted queries across region, product, channel, and time.


**Architecture**
<img width="1361" height="315" alt="image" src="https://github.com/user-attachments/assets/f1ed0295-fb0d-4898-873c-7237dd351682" />

**Requirenment**


Python - 


pip install -r requirement.txt

React Router - 

npm install -D tailwindcss postcss autoprefixer

npm exec tailwindcss@3.4.17 init -p

npx shadcn@latest init

npx shadcn@latest add field

npx shadcn@latest add input

npx shadcn@latest add button

npx shadcn@latest add popover 

**How to run it**

Backend Server -

alembic upgrade head

uvicon main:app --reload


Frontend Server -

cd frontend

npm run dev

