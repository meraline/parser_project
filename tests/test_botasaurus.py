from botasaurus import Browser, browser


@browser
def my_task(url: str, browser: Browser):
    # Go to the website
    browser.goto(url)

    # Get the title
    title = browser.title

    return title


if __name__ == "__main__":
    title = my_task("https://drom.ru")
    print(f"Title: {title}")
