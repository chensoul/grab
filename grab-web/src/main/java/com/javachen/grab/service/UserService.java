package com.javachen.grab.service;

import com.javachen.grab.model.domain.User;
import com.javachen.grab.model.request.LoginUserRequest;
import com.javachen.grab.model.request.RegisterUserRequest;
import com.javachen.grab.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;


    @Transactional
    public User registerUser(RegisterUserRequest request){
        User user = new User();
        user.setUsername(request.getUsername());
        user.setPassword(request.getPassword());
        user.setFirst(true);
        user.setTimestamp(System.currentTimeMillis());
        return userRepository.save(user);
    }

    public User loginUser(LoginUserRequest request){
        User user = findByUsername(request.getUsername());
        if(null == user) {
            return null;
        }else if(!user.passwordMatch(request.getPassword())){
            return null;
        }
        return user;
    }

    public boolean checkUserExist(String username){
        return null != findByUsername(username);
    }

    public User findByUsername(String username){
        User user = userRepository.findByUsername(username);
        return user;
    }

    @Transactional
    public void updateUser(User user){
        userRepository.save(user);
    }

    public User findByUid(Long uid){
       return userRepository.findByUid(uid);
    }

    @Transactional
    public void removeUser(String username){
        User user=findByUsername(username);
        if(null != user) {
            userRepository.delete(user);
        }
    }

}
