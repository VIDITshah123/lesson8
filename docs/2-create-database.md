---
description: 
---

1. Based on the requirements document, create a database structure for the application. 
2. Assume that it is a relational database. 
3. Use sqlite for non-prod and postgresQL for production
4. Generate the tables with meaningful names. 
5. Have tablename_id as the primary key for all tables.
6. For master data , create tables with suffix _master
7. All tables that store transactional data use _tx suffix
8. Maintain foreign key relations where possible
9. Generate a relationship diagram and add it to help page
10. Explain the tables and their use in the help page. Have a section named "Database Info" in the documentation.