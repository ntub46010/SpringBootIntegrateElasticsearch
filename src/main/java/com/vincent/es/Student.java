package com.vincent.es;

import java.util.Date;
import java.util.List;

public class Student {
    private String id;
    private String name;
    private List<String> departments;
    private List<Course> courses;
    private int grade;
    private int conductScore;
    private Job job;
    private String introduction;
    private Date englishIssuedDate;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getDepartments() {
        return departments;
    }

    public void setDepartments(List<String> departments) {
        this.departments = departments;
    }

    public List<Course> getCourses() {
        return courses;
    }

    public void setCourses(List<Course> courses) {
        this.courses = courses;
    }

    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public int getConductScore() {
        return conductScore;
    }

    public void setConductScore(int conductScore) {
        this.conductScore = conductScore;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public String getIntroduction() {
        return introduction;
    }

    public void setIntroduction(String introduction) {
        this.introduction = introduction;
    }

    public Date getEnglishIssuedDate() {
        return englishIssuedDate;
    }

    public void setEnglishIssuedDate(Date englishIssuedDate) {
        this.englishIssuedDate = englishIssuedDate;
    }

    private static class Course {
        private String name;
        private int point;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPoint() {
            return point;
        }

        public void setPoint(int point) {
            this.point = point;
        }
    }

    private static class Job {
        private String name;
        private Boolean primary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getPrimary() {
            return primary;
        }

        public void setPrimary(Boolean primary) {
            this.primary = primary;
        }
    }
}
