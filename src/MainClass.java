import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

public class MainClass {
    public final static String textsFilepath = "./jsons/jsonTexts.json";
    public final static String linksFilepath = "./jsons/jsonLinks.json";
    public final static String picturesFilepath = "./jsons/jsonPictures.json";
    private final static String fourthThreadName = "FourthThread";
    private final static String[] jsonFiles = {textsFilepath, linksFilepath, picturesFilepath};
    private final static String[] threadNames = {"TextsFileThread", "LinksFileThread", "PicturesFileThread"};
    public final static String lockFilePath = "./jsons/isBusy";

    private static FirefoxDriver initialize() {
        System.setProperty("webdriver.gecko.driver","./geckodriver");
        FirefoxOptions options = new FirefoxOptions();
        options.setBinary("/usr/bin/firefox");
        return new FirefoxDriver(options);
    }

    private static class MyThread<T> extends Thread {

        private String filePath;
        private boolean reading;
        private HashMap<String, T> container;

        public MyThread(String ThreadName, String filePath, boolean reading) {
            super(ThreadName);
            this.filePath = filePath;
            this.reading = reading;
            container = new HashMap<>();
        }

        public void setContainer(HashMap<String, T> container) {
            this.container = container;
        }

        public void run() {
            try {
                if (reading)
                    System.out.printf("%s reads %s%n", getName(), filePath);
                else if (!container.isEmpty()) {
                    JSONParser parser = new JSONParser();
                    JSONArray array = (JSONArray) parser.parse(new FileReader(filePath));

                    // Remove all duplicate news from parsed container
                    for (Object record : array)
                        for (Object key : ((JSONObject) record).keySet())
                            container.remove(key);

                    // Forming a new json object
                    JSONObject record = new JSONObject();
                    for (String key : container.keySet())
                        record.put(key, container.get(key));
                    if (!record.isEmpty())
                        array.add(record);

                    FileWriter file = new FileWriter(filePath);
                    file.write(array.toJSONString());
                    file.close();
                }
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
        }
    }

    private static void login(FirefoxDriver driver) throws IOException {
        WebElement phoneInput = driver.findElement(By.id("index_email"));
        WebElement passInput = driver.findElement(By.id("index_pass"));

        BufferedReader br = new BufferedReader(new FileReader("./loginInfo.txt"));
        phoneInput.sendKeys(br.readLine());
        passInput.sendKeys(br.readLine());
        br.close();

        WebElement loginButton = driver.findElement(By.id("index_login_button"));
        loginButton.click();
    }

    private static void checkFilesExistence() throws IOException {
        if (Files.notExists(Path.of("./jsons")))
            new File("./jsons").mkdirs();

        for (String path : jsonFiles)
            if (Files.notExists(Path.of(path)))
                Files.write(Paths.get(path), "".getBytes());
    }

    private static void planAhead(ArrayList<Queue<MyThread>> schedule, int iterations_number) {
        int fourthThreadQueueIndex = 0;
        for(int i = 0; i < iterations_number * 3; ++i) {
            for (int j = 0; j < 3; ++j) {
                MyThread nextThread;
                if (j == fourthThreadQueueIndex % 3)
                    nextThread = new MyThread(fourthThreadName, jsonFiles[j], true);
                else
                    nextThread = new MyThread(threadNames[j], jsonFiles[j], false);
                schedule.get(j).add(nextThread);
            }
            ++fourthThreadQueueIndex;
        }
        for (int i = 0; i < 3; ++i)
            schedule.get(i).add(new MyThread(threadNames[i], jsonFiles[i], false));
    }

    private static void occupyFiles() throws IOException, InterruptedException {
        // Checking if files are not occupied
        boolean isOccupied = true;
        while (isOccupied) {
            FileReader fr = new FileReader(lockFilePath);
            isOccupied = (fr.read() == '1');
            fr.close();
            if (isOccupied)
                Thread.sleep(100);
        }
        FileWriter fr = new FileWriter(lockFilePath);
        fr.write('1');
        fr.close();
    }

    private static void freeFiles() throws IOException {
        FileWriter fr = new FileWriter(lockFilePath);
        fr.write('0');
        fr.close();
    }

    private static void parseFeed(FirefoxDriver driver) throws IOException, InterruptedException {
        checkFilesExistence();
        occupyFiles();

        Set<String> postsIds = new HashSet<>();

        // Planning
        ArrayList<Queue<MyThread>> schedule = new ArrayList<>();
        for (int i = 0; i < 3; ++i)
            schedule.add(new LinkedList<>());

        // Change this variable value to test the program.
        // -----------------------------------------------
        int iterationsNumber = 100;
        // -----------------------------------------------

        // Vc can't handle more than 19 scrolls down
        iterationsNumber = min(iterationsNumber, 19);

        planAhead(schedule, iterationsNumber);

        HashMap<String, String> texts = new HashMap<>();
        HashMap<String, JSONArray> links = new HashMap<>();
        HashMap<String, JSONArray> pictures = new HashMap<>();

        for (int i = 1; i <= iterationsNumber; ++i) {

            List<WebElement> feedRows = driver.findElements(By.className("feed_row"));

            for (WebElement post : feedRows) {
                boolean isPostPresent = post.findElements(By.className("post")).size() > 0;
                if (isPostPresent) {
                    String id = post.findElement(By.className("post")).getAttribute("id");
                if (!postsIds.contains(id)) {
                        postsIds.add(id);

                        // texts
                        boolean isExpandTextPresent = post.findElements(By.className("wall_post_more")).size() > 0;
                        if (isExpandTextPresent)
                            post.findElement(By.className("wall_post_more")).click();

                        String text = post.findElement(By.className("wall_text")).getText();
                        if (!text.equals(""))
                            texts.put(id, text);

                        // links
                        List<WebElement> elements = post.findElements(By.tagName("a"));
                        Set<String> linkSet = new HashSet<>();
                        for (WebElement elem : elements) {
                            String link = elem.getAttribute("href");
                            if (link != null && !link.equals(""))
                                linkSet.add(link);
                        }
                        ArrayList<String> linkList = new ArrayList<>(linkSet);
                        JSONArray jsonArray = new JSONArray();
                        jsonArray.addAll(linkList);
                        if (!linkList.isEmpty())
                            links.put(id, jsonArray);

                        // pictures
                        ArrayList<String> pictureList = new ArrayList<>();
                        elements = post.findElements(By.className("page_post_thumb_wrap"));
                        for (WebElement elem : elements)
                            pictureList.add(elem.getAttribute("Style").split("url")[1].replaceAll("[\"();]+", ""));
                        jsonArray = new JSONArray();
                        jsonArray.addAll(pictureList);
                        if (!pictureList.isEmpty())
                            pictures.put(id, jsonArray);
                    }
                }
            }

            // Internet is too slow
            Thread.sleep(1000);
            WebElement showMoreButton = driver.findElement(By.id("show_more_link"));
            showMoreButton.click();

            // Executing planned actions

            ArrayList<MyThread> startedThreads = new ArrayList<>();
            for (int j = 0; j < 3; ++j) {
                MyThread thread = schedule.get(j).peek();
                if (!thread.getName().equals(fourthThreadName)) {
                    switch (j) {
                        case 0:
                            thread.setContainer(texts);
                            texts = new HashMap<>();
                            break;
                        case 1:
                            thread.setContainer(links);
                            links = new HashMap<>();
                            break;
                        case 2:
                            thread.setContainer(pictures);
                            pictures = new HashMap<>();
                    }
                }
                thread.start();
                startedThreads.add(thread);
                schedule.get(j).remove();
            }
            for (MyThread thread : startedThreads)
                thread.join();
            // Free files for some time for other processes
            if (i % 5 == 0) {
                freeFiles();
                Thread.sleep(100);
                occupyFiles();
            }
        }

        // Writing iteration without fourth thread
        ArrayList<MyThread> startedThreads = new ArrayList<>();
        for(int i = 0; i < 3; ++i) {
            MyThread thread = schedule.get(i).peek();
            thread.start();
            startedThreads.add(thread);
        }
        for (MyThread thread : startedThreads)
            thread.join();

        freeFiles();
    }

    public static void main(String[] args) throws Exception {
        FirefoxDriver driver = initialize();

        driver.get("https://vk.com");
        login(driver);

        driver.get("https://vk.com/feed");
        parseFeed(driver);

        // Driver may not finish correctly if shut down immediately
        Thread.sleep(1000);
        driver.quit();
    }
}
