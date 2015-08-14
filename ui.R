library(shinydashboard)

messageMenu <- dropdownMenu(type = "messages",
  messageItem(
    from = "Sales Dept",
    message = "Sales are steady this month."
  ),
  messageItem(
    from = "New User",
    message = "How do I register?",
    icon = icon("question"),
    time = "13:45"
  ),
  messageItem(
    from = "Support",
    message = "The new server is ready.",
    icon = icon("life-ring"),
    time = "2014-12-01"
  )
)

notificationMenu <- dropdownMenu(type = "notifications",
  notificationItem(
    text = "5 new users today",
    icon("users")
  ),
  notificationItem(
    text = "12 items delivered",
    icon("truck"),
    status = "success"
  ),
  notificationItem(
    text = "Server load at 86%",
    icon = icon("exclamation-triangle"),
    status = "warning"
  )
)

taskMenu <- dropdownMenu(type = "tasks", badgeStatus = "success",
  taskItem(value = 90, color = "green",
    "Documentation"
  ),
  taskItem(value = 17, color = "aqua",
    "Project X"
  ),
  taskItem(value = 75, color = "yellow",
    "Server deployment"
  ),
  taskItem(value = 80, color = "red",
    "Overall project"
  )
)

header <- dashboardHeader(title = "Folio",
                          messageMenu, notificationMenu, taskMenu)

sidebar <- dashboardSidebar(
  sidebarMenu(
    menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
    menuItem("Widgets", tabName = "widgets", icon = icon("th"))
  )
)

body <- dashboardBody(
  tabItems(
    # First tab content
    tabItem(tabName = "dashboard",
            fluidRow(
              box(title = "Histogram", status = "primary", plotOutput("plot2", height = 250)),
              
              box(
                title = "Inputs", status = "warning",
                "Box content here", br(), "More box content",
                sliderInput("slider", "Slider input:", 1, 100, 50),
                textInput("text", "Text input:")
              )
            )
    ),
    
    # Second tab content
    tabItem(tabName = "widgets",
            h2("Widgets tab content")
    )
  )
)

dashboardPage(header, sidebar, body)