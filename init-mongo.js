// Create and switch to the Database
db = db.getSiblingDB("fombook");

// Create a collections
db.createCollection("users");
db.users.createIndex({ email: 1 }, { unique: true });

db.createCollection("posts");
db.posts.createIndex({ group_id: 1 }, { unique: false });

db.createCollection("comments");
db.comments.createIndex({ post_id: 1 }, { unique: false });

db.createCollection("groups");
db.groups.insertMany([
    {
        name: "All Students",
        description: "Group for all Students",
        user_type: "student",
        owner_id: null,
        is_default: true,
        id_private: true,
        created_at: new Date(),
        updated_at: new Date(),
    },
    {
        name: "All Alumni",
        description: "Group for all Alumni",
        user_type: "alumni",
        owner_id: null,
        is_default: true,
        is_private: true,
        created_at: new Date(),
        updated_at: new Date(),
    },
    {
        name: "Everyone",
        description: "Group for everyone",
        user_type: null,
        owner_id: null,
        is_default: true,
        is_private: false,
        created_at: new Date(),
        updated_at: new Date(),
    },
]);

db.createCollection("studies");
db.studies.insertMany([
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Angewandte Künstliche Intelligenz",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Arbeits-, Organisations- und Personalpsychologie",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Arbeitsrecht für die Unternehmenspraxis",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Business Administration",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Business Administration - dual kompakt",
    },
    {
        degree: "Master of Business Administration (MBA)",
        study: "Business Administration",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Business Consulting & Digital Management",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Business Psychology",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Coaching, Beratung & Change",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Cyber Security",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Cyber Security Management",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Cyber Security Management",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Digitalisierung & Management",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Finance & Accounting",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Finance & Banking",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Future Management",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Gesundheitspsychologie & Medizinpädagogik",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Informatik",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "International Business Management",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "International Management",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "IT Management",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "KI & Business Analytics",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Human Resource Management",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Leadership",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Management & Digitalisierung",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Management im Gesundheitswesen",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Management in der Gefahrenabwehr",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Managing Global Dynamics",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Marketing & Digital Media",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Marketing & Digitale Medien",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Marketing- und Brand Management",
    },
    {
        degree: "Bachelor of Engineering (B.Eng.)",
        study: "Maschinenbau & Digitale Technologien",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Medizinmanagement",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Nachhaltigkeitsmanagement",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Pädagogik & Digitales Lernen",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Pflege",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Pflege & Digitalisierung",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Pflegemanagement",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Primärmedizinisches Versorgungs- und Praxismanagement",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Public Health",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Real Estate Management",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Recht & Management",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Risk Management & Treasury",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Sales Management",
    },
    {
        degree: "Bachelor of Arts (B.A.)",
        study: "Soziale Arbeit",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Sozialmanagement",
    },
    {
        degree: "Bachelor of Laws (LL.B.)",
        study: "Steuerrecht",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Supply Chain Management",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Sustainability & Business Transformation",
    },
    {
        degree: "Master of Laws (LL.M.)",
        study: "Taxation",
    },
    {
        degree: "Master of Science (M.Sc.)",
        study: "Unternehmensführung & Controlling",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "UX-Design & Digital Solutions",
    },
    {
        degree: "Master of Arts (M.A.)",
        study: "Wirtschaft & Management",
    },
    {
        degree: "Bachelor of Science (B.Sc.)",
        study: "Wirtschaftsinformatik",
    },
]);
